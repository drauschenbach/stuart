local class = require 'stuart.class'
require 'stuart.streaming.DStream'

local TransformedDStream, parent = class.new('TransformedDStream', 'DStream')

function TransformedDStream:__init(ssc, transformFunc)
  parent.__init(self, ssc)
  self.transformFunc = transformFunc
end

function TransformedDStream:_notify(validTime, rdd)
  rdd = self.transformFunc(rdd, validTime)
  for _, dstream in ipairs(self.inputs) do
    rdd = dstream:_notify(validTime, rdd)
  end
  for _, dstream in ipairs(self.outputs) do
    dstream:_notify(validTime, rdd)
  end
  return rdd
end

return TransformedDStream
