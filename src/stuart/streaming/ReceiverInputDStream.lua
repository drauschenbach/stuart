local class = require 'stuart.class'
require 'stuart.streaming.DStream'

local ReceiverInputDStream, parent = class.new('ReceiverInputDStream', 'DStream')

function ReceiverInputDStream:initialize(ssc, receiver)
  parent.__init(self, ssc)
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
