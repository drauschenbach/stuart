local class = require 'middleclass'
local DStream = require 'stuart.streaming.DStream'

local ReceiverInputDStream = class('ReceiverInputDStream', DStream)

function ReceiverInputDStream:initialize(ssc, receiver)
  DStream.initialize(self, ssc)
  self.receiver = receiver
end

function ReceiverInputDStream:compute(durationBudget)
  self.receiver:run(durationBudget)
end

function DStream:start()
  self.receiver:onStart()
end

function DStream:stop()
  self.receiver:onStop()
end

return ReceiverInputDStream
