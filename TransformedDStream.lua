local class = require 'middleclass'
local DStream = require 'DStream'

local TransformedDStream = class('TransformedDStream', DStream)

function TransformedDStream:initialize(ctx, transformFunc)
  DStream.initialize(self, ctx)
  self.transformFunc = transformFunc
end

function TransformedDStream:_notify(validTime, rdd)
  rdd = self.transformFunc(rdd)
  for i, dstream in ipairs(self.inputs) do
    rdd = dstream:_notify(validTime, rdd)
  end
  for i, dstream in ipairs(self.outputs) do
    dstream:_notify(validTime, rdd)
  end
  return rdd
end

return TransformedDStream
