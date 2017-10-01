local class = require 'middleclass'
local DStream = require 'stuart.streaming.DStream'

local TransformedDStream = class('TransformedDStream', DStream)

function TransformedDStream:initialize(ssc, transformFunc)
  DStream.initialize(self, ssc)
  self.transformFunc = transformFunc
end

function TransformedDStream:_notify(validTime, rdd)
  rdd = self.transformFunc(rdd)
  for _, dstream in ipairs(self.inputs) do
    rdd = dstream:_notify(validTime, rdd)
  end
  for _, dstream in ipairs(self.outputs) do
    dstream:_notify(validTime, rdd)
  end
  return rdd
end

return TransformedDStream
