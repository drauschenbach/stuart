local class = require 'stuart.class'
local DStream = require 'stuart.streaming.DStream'

local ReceiverInputDStream = class.new(DStream)

function ReceiverInputDStream:_init(ssc, receiver)
  DStream._init(self, ssc)
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
