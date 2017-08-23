local class = require 'middleclass'
local DStream = require 'DStream'
local socket = require 'socket'

local SocketInputDStream = class('SocketInputDStream', DStream)

function SocketInputDStream:initialize(ctx, hostname, port)
  DStream.initialize(self, ctx)
  self.hostname = hostname
  self.port = port or 0
end

function SocketInputDStream:compute(durationBudget)
  
  -- Connect
  local conn = socket.connect(self.hostname, self.port)
  
  -- run loop. Read multiple lines until the duration budget has elapsed
  local timeOfLastYield = socket.gettime()
  local rdds = {}
  local minWait = 0.02 -- never block less than a 20ms "average context switch"
  while true do
    local elapsed = socket.gettime() - timeOfLastYield
    if elapsed > durationBudget then
      coroutine.yield(rdds)
      rdds = {}
      timeOfLastYield = socket.gettime()
    else
      conn:settimeout(math.max(minWait, durationBudget - elapsed))
      local line, err = conn:receive('*l')
      if not err then
        rdds[#rdds+1] = self.sc:makeRDD({line})
      end
    end
  end
  
end

return SocketInputDStream
