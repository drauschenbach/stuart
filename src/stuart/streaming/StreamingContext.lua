local class = require 'middleclass'
local moses = require 'moses'
local socket = require 'socket'

local RDD = require 'stuart.RDD'
local QueueInputDStream = require 'stuart.streaming.QueueInputDStream'
local ReceiverInputDStream = require 'stuart.streaming.ReceiverInputDStream'
local SocketInputDStream = require 'stuart.streaming.SocketInputDStream'

local function sleep(timeout)
  socket.select(nil, nil, timeout)
end

local function isInstanceOf(x, aClass)
  if not moses.has(x, 'isInstanceOf') then return false end
  return x:isInstanceOf(aClass)
end

-------------------------------------------------------------------------------

local StreamingContext = class('StreamingContext')

function StreamingContext:initialize(sc, batchDuration)
  self.sc = sc
  self.batchDuration = batchDuration or 1
  self.dstreams={}
  self.state='initialized'
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
  rdds = moses.map(rdds, function(i,rdd)
    if not isInstanceOf(rdd, RDD) then rdd = self.sc:makeRDD(rdd) end
    return rdd
  end)
  local dstream = QueueInputDStream:new(self, rdds)
  self.dstreams[#self.dstreams+1] = dstream
  return dstream
end

function StreamingContext:receiverStream(receiver)
  local dstream = ReceiverInputDStream:new(self, receiver)
  self.dstreams[#self.dstreams+1] = dstream
  return dstream
end

function StreamingContext:socketTextStream(hostname, port)
  local dstream = SocketInputDStream:new(self, hostname, port)
  self.dstreams[#self.dstreams+1] = dstream
  return dstream
end

function StreamingContext:start()
  if self.state == 'stopped' then error('StreamingContext has already been stopped') end
  for i, dstream in ipairs(self.dstreams) do
    dstream:start()
  end
  self.state = 'active'
end

function StreamingContext:stop()
  for i, dstream in ipairs(self.dstreams) do
    dstream:stop()
  end
  self.state = 'stopped'
end

return StreamingContext
