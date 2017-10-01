local class = require 'middleclass'
local ReceiverInputDStream = require 'stuart.streaming.ReceiverInputDStream'
local SocketReceiver = require 'stuart.streaming.SocketReceiver'

local SocketInputDStream = class('SocketInputDStream', ReceiverInputDStream)

function SocketInputDStream:initialize(ssc, hostname, port)
  local receiver = SocketReceiver:new(ssc, hostname, port)
  ReceiverInputDStream.initialize(self, ssc, receiver)
end

return SocketInputDStream
