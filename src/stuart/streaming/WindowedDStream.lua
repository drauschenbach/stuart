local class = require 'middleclass'
local DStream = require 'stuart.streaming.DStream'

local WindowedDStream = class('WindowedDStream', DStream)

function WindowedDStream:initialize(ssc, windowDuration, slideDuration)
  DStream.initialize(self, ssc)
  self.windowDuration = windowDuration
  self.slideDuration = slideDuration
  self.window = {}
end

function WindowedDStream:_notify(validTime, rdd)
  
  -- expire items from the window
  while #self.window > 0 and validTime - self.window[1]._validTime > self.windowDuration do
    table.remove(self.window, 1)
  end
  
  -- add current rdd to the window
  rdd._validTime = validTime
  self.window[#self.window+1] = rdd
  local unioned = self.ssc.sc:union(self.window)
  
  for _, dstream in ipairs(self.inputs) do
    rdd = dstream:_notify(validTime, unioned)
  end

  for _, dstream in ipairs(self.outputs) do
    dstream:_notify(validTime, unioned)
  end
  
  return rdd
end

return WindowedDStream
