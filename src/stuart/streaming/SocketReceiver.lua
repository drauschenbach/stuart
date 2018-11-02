local class = require 'middleclass'
local Receiver = require 'stuart.streaming.Receiver'

local SocketReceiver = class('SocketReceiver', Receiver)

function SocketReceiver:initialize(ssc, hostname, port)
  local has_luasocket, _ = pcall(require, 'socket')
  assert(has_luasocket)
  Receiver.initialize(self, ssc)
  self.hostname = hostname
  self.port = port or 0
end

function SocketReceiver:onStart()
  local log = require 'stuart.internal.logging'.log
  log:info(string.format('Connecting to %s:%d', self.hostname, self.port))
  local socket = require 'socket'
  self.conn, self.err = socket.connect(self.hostname, self.port)
  if self.err then
    log:error(string.format('Error connecting to %s:%d: %s', self.hostname, self.port, self.err))
    return
  end
  log:info(string.format('Connected to %s:%d', self.hostname, self.port))
end

function SocketReceiver:onStop()
  if self.conn ~= nil then self.conn:close() end
end

function SocketReceiver:poll(durationBudget)
  local now = require 'stuart.interface'.now
  local startTime = now()
  local data = {}
  local minWait = 0.01
  while true do
    local elapsed = now() - startTime
    if elapsed > durationBudget then break end
    
    self.conn:settimeout(math.max(minWait, durationBudget - elapsed))
    local line, err = self.conn:receive('*l')
    if not err then
      data[#data+1] = line
    end
  end
  return self.ssc.sc:makeRDD(data)
end

return SocketReceiver
