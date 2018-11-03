local class = require 'stuart.util.class'
local ReceiverInputDStream = require 'stuart.streaming.ReceiverInputDStream'
local SocketReceiver = require 'stuart.streaming.SocketReceiver'

local SocketInputDStream, parent = class('SocketInputDStream', class.type(ReceiverInputDStream))

function SocketInputDStream:initialize(ssc, hostname, port)
  local receiver = SocketReceiver.new(ssc, hostname, port)
  parent.__init(self, ssc, receiver)
end

return SocketInputDStream
