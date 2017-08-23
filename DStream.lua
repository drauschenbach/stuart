local class = require 'middleclass'

local DStream = class('DStream')

function DStream:initialize(ctx)
  self.ctx = ctx
  self.inputs = {}
  self.outputs = {}
end

function DStream:_notify(validTime, rdd)
  for i, dstream in ipairs(self.inputs) do
    rdd = dstream:_notify(validTime, rdd)
  end
  for i, dstream in ipairs(self.outputs) do
    dstream:_notify(validTime, rdd)
  end
end

function DStream:count()
  local transformFunc = function(rdd)
    return self.ctx:makeRDD({rdd:count()})
  end
  return self:transform(transformFunc)
end

function DStream:foreachRDD(foreachFunc)
  local TransformedDStream = require 'TransformedDStream'
  local dstream = TransformedDStream:new(self.ctx, foreachFunc)
  self.outputs[#self.outputs+1] = dstream
  return self
end

function DStream:transform(transformFunc)
  local TransformedDStream = require 'TransformedDStream'
  local dstream = TransformedDStream:new(self.ctx, transformFunc)
  self.inputs[#self.inputs+1] = dstream
  return dstream
end

return DStream
