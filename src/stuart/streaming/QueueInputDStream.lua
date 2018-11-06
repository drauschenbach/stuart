local class = require 'stuart.util.class'
local DStream = require 'stuart.streaming.DStream'

local QueueInputDStream, parent = class.new('QueueInputDStream', class.type(DStream))

function QueueInputDStream:__init(ssc, rdds, oneAtATime)
  parent.__init(self, ssc)
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
