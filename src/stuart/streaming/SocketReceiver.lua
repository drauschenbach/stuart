local class = require 'middleclass'
local clock = require 'stuart.interface.clock'
local log = require 'stuart.internal.logging'.log
local has_luasocket, socket = pcall(require, 'socket')
local Receiver = require 'stuart.streaming.Receiver'

local SocketReceiver = class('SocketReceiver', Receiver)

function SocketReceiver:initialize(ssc, hostname, port)
  assert(has_luasocket)
  Receiver.initialize(self, ssc)
  self.hostname = hostname
  self.port = port or 0
end

function SocketReceiver:onStart()
  log:info(string.format('Connecting to %s:%d', self.hostname, self.port))
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

function SocketReceiver:run(durationBudget)
  
  -- run loop. Read multiple lines until the duration budget has elapsed
  local timeOfLastYield = clock.now()
  local data = {}
  local minWait = 0.02 -- never block less than a 20ms "average context switch"
  while true do
    local elapsed = clock.now() - timeOfLastYield
    if elapsed > durationBudget then
      local rdd = nil
      if #data > 0 then rdd = self.ssc.sc:makeRDD(data) end
      coroutine.yield({rdd})
      data = {}
      timeOfLastYield = clock.now()
    else
      self.conn:settimeout(math.max(minWait, durationBudget - elapsed))
      local line, err = self.conn:receive('*l')
      if not err then
        data[#data+1] = line
      end
    end
  end
  
end

return SocketReceiver
