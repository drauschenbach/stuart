local _ = require 'lodash'
local DStream = require 'DStream'
local socket = require 'socket'

local SocketInputDStream = DStream:new({hostname=nil, port=0})

function SocketInputDStream:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function SocketInputDStream:compute(durationBudget)
  
  -- Connect
  local conn = socket.connect(self.hostname, self.port)
  conn:settimeout(durationBudget)
  
  -- run loop
  while true do
    local line, err = conn:receive('*l')
    local rdds
    if not err then
      rdds = {self.sc:makeRDD({line})}
    end
    coroutine.yield(rdds)
  end
  
end

return SocketInputDStream
