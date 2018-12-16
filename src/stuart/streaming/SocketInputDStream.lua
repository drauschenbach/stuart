local class = require 'stuart.class'
local ReceiverInputDStream = require 'stuart.streaming.ReceiverInputDStream'

local SocketInputDStream = class.new(ReceiverInputDStream)

function SocketInputDStream:_init(ssc, hostname, port)
  local SocketReceiver = require 'stuart.streaming.SocketReceiver'
  local receiver = SocketReceiver.new(ssc, hostname, port)
  ReceiverInputDStream._init(self, ssc, receiver)
end

return SocketInputDStream
