local class = require 'middleclass'
local ReceiverInputDStream = require 'ReceiverInputDStream'
local SocketReceiver = require 'SocketReceiver' 

local SocketInputDStream = class('SocketInputDStream', ReceiverInputDStream)

function SocketInputDStream:initialize(ssc, hostname, port)
  local receiver = SocketReceiver:new(ssc, hostname, port)
  ReceiverInputDStream.initialize(self, ssc, receiver)
end

return SocketInputDStream
