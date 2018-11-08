local class = require 'stuart.class'
require 'stuart.streaming.DStream'
local SocketReceiver = require 'stuart.streaming.SocketReceiver'

local SocketInputDStream, parent = class.new('SocketInputDStream', 'ReceiverInputDStream')

function SocketInputDStream:initialize(ssc, hostname, port)
  local receiver = SocketReceiver:new(ssc, hostname, port)
  parent.__init(self, ssc, receiver)
end

return SocketInputDStream
