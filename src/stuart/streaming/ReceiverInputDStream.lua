local class = require 'stuart.class'
local DStream = require 'stuart.streaming.DStream'

local ReceiverInputDStream = class.new(DStream)

function ReceiverInputDStream:__init(ssc, receiver)
  self:super(ssc)
  self.receiver = receiver
end

function ReceiverInputDStream:poll(durationBudget)
  return self.receiver:poll(durationBudget)
end

function ReceiverInputDStream:start()
  self.receiver:onStart()
end

function ReceiverInputDStream:stop()
  self.receiver:onStop()
end

return ReceiverInputDStream
