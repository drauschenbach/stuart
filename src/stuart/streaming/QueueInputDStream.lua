local class = require 'middleclass'

local DStream = require 'stuart.streaming.DStream'

local QueueInputDStream = class('QueueInputDStream', DStream)

function QueueInputDStream:initialize(ssc, rdds, oneAtATime)
  DStream.initialize(self, ssc)
  self.queue = rdds
  self.oneAtATime = oneAtATime
end

function QueueInputDStream:compute()
  while true do
  
    if self.oneAtATime then
      local rdd = table.remove(self.queue, 1)
      coroutine.yield({rdd})
    
    else
      local rdds = self.queue
      self.queue = {}
      coroutine.yield(rdds)
    end
    
  end
end

return QueueInputDStream
