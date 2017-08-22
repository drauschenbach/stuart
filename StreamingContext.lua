local moses = require 'moses'
local QueueInputDStream = require 'QueueInputDStream'
local socket = require 'socket'
local SocketInputDStream = require 'SocketInputDStream'

local function sleep(timeout)
  socket.select(nil, nil, timeout)
end

StreamingContext = {sc=nil, batchDuration=1, dstreams={}, state='initialized'}

function StreamingContext:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function StreamingContext:awaitTerminationOrTimeout(timeout)
  if not moses.isNumber(timeout) or timeout <= 0 then error('Invalid timeout') end
  
  local coroutines = {}
  for i,dstream in ipairs(self.dstreams) do
    coroutines[#coroutines+1] = {coroutine.create(dstream.compute), dstream}
  end
  
  -- run loop
  local startTime = socket.gettime()
  local loopDurationGoal = 0.05 -- 50ms
  local individualDStreamDurationBudget = loopDurationGoal / #self.dstreams 
  while self.state == 'active' do
  
    -- Decide whether to timeout
    local now = socket.gettime()
    local elapsed = now - startTime
    if elapsed > timeout then break end
    local loopStartTime = now
    
    -- Run each dstream compute() function, until it yields
    for i,copair in ipairs(coroutines) do
      local co = copair[1]
      local dstream = copair[2]
      if coroutine.status(co) == 'suspended' then
        local ok, rdds = coroutine.resume(co, dstream, individualDStreamDurationBudget) --, now, individualDStreamDurationBudget)
        if ok and (rdds ~= nil) and (#rdds > 0) then
          for i, rdd in ipairs(rdds) do dstream:_notify(now, rdd) end
        end
      end
    end
    
    sleep(loopDurationGoal)
  end
  --moses.print('Ending run loop')
end

function StreamingContext:getState()
  return self.state
end

function StreamingContext:queueStream(rdds, oneAtATime)
  if not moses.isBoolean(oneAtATime) then oneAtATime = true end
  local dstream = QueueInputDStream:new({ctx=self.sc, batchDuration=self.batchDuration, queue=rdds})
  self.dstreams[#self.dstreams+1] = dstream
  return dstream
end

function StreamingContext:socketTextStream(hostname, port)
  if not moses.isBoolean(oneAtATime) then oneAtATime = true end
  local dstream = SocketInputDStream:new{ctx=self.sc, hostname=hostname, port=port}
  self.dstreams[#self.dstreams+1] = dstream
  return dstream
end

function StreamingContext:start()
  if self.state == 'stopped' then error('StreamingContext has already been stopped') end
  self.state = 'active'
end

function StreamingContext:stop()
  self.state = 'stopped'
end

return StreamingContext
