local class = require 'middleclass'

local DStream = require 'stuart.streaming.DStream'

local QueueInputDStream = class('QueueInputDStream', DStream)

function QueueInputDStream:initialize(ssc, rdds, oneAtATime)
  DStream.initialize(self, ssc)
  self.queue = rdds
  self.oneAtATime = oneAtATime
end

function QueueInputDStream:poll()
  if self.oneAtATime then
    return {table.remove(self.queue, 1)}
  else
    return self.queue
  end
end

return QueueInputDStream
