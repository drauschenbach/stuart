local class = require 'stuart.class'

local DStream = class.new()

function DStream:_init(ssc)
  self.ssc = ssc
  self.inputs = {}
  self.outputs = {}
end

function DStream:_notify(validTime, rdd)
  for _, dstream in ipairs(self.inputs) do
    rdd = dstream:_notify(validTime, rdd)
  end
  for _, dstream in ipairs(self.outputs) do
    dstream:_notify(validTime, rdd)
  end
end

function DStream:count()
  local transformFunc = function(rdd)
    return self.ssc.sc:makeRDD({rdd:count()})
  end
  return self:transform(transformFunc)
end

function DStream:countByWindow(windowDuration)
  return self:window(windowDuration):count()
end

function DStream:foreachRDD(foreachFunc)
  local TransformedDStream = require 'stuart.streaming.TransformedDStream'
  local dstream = TransformedDStream.new(self.ssc, foreachFunc)
  self.outputs[#self.outputs+1] = dstream
end

function DStream:groupByKey()
  local transformFunc = function(rdd) return rdd:groupByKey() end
  return self:transform(transformFunc)
end

function DStream:map(f)
  local transformFunc = function(rdd)
    return rdd:map(f)
  end
  return self:transform(transformFunc)
end

function DStream:mapValues(f)
  local transformFunc = function(rdd)
    return rdd:mapValues(f)
  end
  return self:transform(transformFunc)
end

function DStream:poll()
end

function DStream:reduce(f)
  local transformFunc = function(rdd)
    return rdd:map(function(x) return {0, x} end)
      :reduceByKey(f)
      :map(function(e) return e[2] end)
  end
  return self:transform(transformFunc)
end

function DStream:start()
end

function DStream:stop()
end

function DStream:transform(transformFunc)
  local TransformedDStream = require 'stuart.streaming.TransformedDStream'
  local dstream = TransformedDStream.new(self.ssc, transformFunc)
  self.inputs[#self.inputs+1] = dstream
  return dstream
end

function DStream:window(windowDuration)
  local WindowedDStream = require 'stuart.streaming.WindowedDStream'
  local dstream = WindowedDStream.new(self.ssc, windowDuration)
  self.inputs[#self.inputs+1] = dstream
  return dstream
end

return DStream
