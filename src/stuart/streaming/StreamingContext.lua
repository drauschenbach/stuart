local class = require 'middleclass'
local clock = require 'stuart.interface.clock'
local isInstanceOf = require 'stuart.util.isInstanceOf'
local moses = require 'moses'
local socket = require 'socket'

local RDD = require 'stuart.RDD'
local QueueInputDStream = require 'stuart.streaming.QueueInputDStream'
local ReceiverInputDStream = require 'stuart.streaming.ReceiverInputDStream'
local SocketInputDStream = require 'stuart.streaming.SocketInputDStream'

local function sleep(timeout)
  socket.select(nil, nil, timeout)
end

-------------------------------------------------------------------------------

local StreamingContext = class('StreamingContext')

function StreamingContext:initialize(sc, batchDuration)
  self.sc = sc
  self.batchDuration = batchDuration or 1
  self.dstreams={}
  self.state='initialized'
  getmetatable(self).conf = sc.conf
  getmetatable(self).sparkContext = sc
end

function StreamingContext:awaitTermination()
  self:awaitTerminationOrTimeout(0)
end

function StreamingContext:awaitTerminationOrTimeout(timeout)
  if not moses.isNumber(timeout) or timeout < 0 then error('Invalid timeout') end
  
  local coroutines = {}
  for _,dstream in ipairs(self.dstreams) do
    coroutines[#coroutines+1] = {coroutine.create(dstream.compute), dstream}
  end
  
  -- run loop
  local startTime = clock.now()
  local loopDurationGoal = self.batchDuration
  local individualDStreamDurationBudget = loopDurationGoal / #self.dstreams
  while self.state == 'active' do
  
    -- Decide whether to timeout
    local now = clock.now()
    if timeout > 0 then
      local elapsed = now - startTime
      if elapsed > timeout then break end
    end
    
    -- Run each dstream compute() function, until it yields
    for _,copair in ipairs(coroutines) do
      local co = copair[1]
      local dstream = copair[2]
      if coroutine.status(co) == 'suspended' then
        local ok, rdds = coroutine.resume(co, dstream, individualDStreamDurationBudget)
        if ok and (rdds ~= nil) and (#rdds > 0) then
          for _, rdd in ipairs(rdds) do dstream:_notify(now, rdd) end
        end
      end
    end
    
    sleep(loopDurationGoal)
  end
  --print('Ending run loop')
end

function StreamingContext:getState()
  return self.state
end

function StreamingContext:queueStream(rdds, oneAtATime)
  if not moses.isBoolean(oneAtATime) then oneAtATime = true end
  rdds = moses.map(rdds, function(_,rdd)
    if not isInstanceOf(rdd, RDD) then rdd = self.sc:makeRDD(rdd) end
    return rdd
  end)
  local dstream = QueueInputDStream:new(self, rdds, oneAtATime)
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
  for _, dstream in ipairs(self.dstreams) do
    dstream:start()
  end
  self.state = 'active'
end

function StreamingContext:stop(stopSparkContext)
  if stopSparkContext == nil then
    stopSparkContext = self.conf:getBoolean('spark.streaming.stopSparkContextByDefault', true)
  end
  for _, dstream in ipairs(self.dstreams) do
    dstream:stop()
  end
  self.state = 'stopped'
  if stopSparkContext then self.sc:stop() end
end

return StreamingContext
