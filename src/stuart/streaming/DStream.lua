local class = require 'middleclass'

local DStream = class('DStream')

function DStream:initialize(ssc)
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

function DStream:foreachRDD(foreachFunc)
  local TransformedDStream = require 'stuart.streaming.TransformedDStream'
  local dstream = TransformedDStream:new(self.ssc, foreachFunc)
  self.outputs[#self.outputs+1] = dstream
  return self
end

function DStream:groupByKey()
  local transformFunc = function(rdd) return rdd:groupByKey() end
  return self:transform(transformFunc)
end

function DStream:mapValues(f)
  local transformFunc = function(rdd)
    return rdd:mapValues(f)
  end
  return self:transform(transformFunc)
end

function DStream:start()
end

function DStream:stop()
end

function DStream:transform(transformFunc)
  local TransformedDStream = require 'stuart.streaming.TransformedDStream'
  local dstream = TransformedDStream:new(self.ssc, transformFunc)
  self.inputs[#self.inputs+1] = dstream
  return dstream
end

return DStream
