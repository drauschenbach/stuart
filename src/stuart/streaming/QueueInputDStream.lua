local class = require 'stuart.class'
local DStream = require 'stuart.streaming.DStream'

local QueueInputDStream = class.new(DStream)

function QueueInputDStream:_init(ssc, rdds, oneAtATime)
  DStream._init(self, ssc)
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
