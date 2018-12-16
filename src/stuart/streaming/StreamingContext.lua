local class = require 'stuart.class'

local StreamingContext = class.new()

function StreamingContext:_init(sc, batchDuration)
  self.sc = sc
  self.sparkContext = sc
  self.batchDuration = batchDuration or 1
  self.dstreams={}
  self.state='initialized'
  self.conf = sc.conf
end

function StreamingContext:awaitTermination()
  self:awaitTerminationOrTimeout(0)
end

function StreamingContext:awaitTerminationOrTimeout(timeout)
  local moses = require 'moses'
  if not moses.isNumber(timeout) or timeout < 0 then error('Invalid timeout') end
  
  
  -- run loop
  local now = require 'stuart.interface'.now
  local startTime = now()
  local loopDurationGoal = self.batchDuration
  local individualDStreamDurationBudget = loopDurationGoal / #self.dstreams
  local sleep = require 'stuart.interface'.sleep
  while self.state == 'active' do
  
    -- Decide whether to timeout
    local currentTime = now()
    if timeout > 0 then
      local elapsed = currentTime - startTime
      if elapsed > timeout then break end
    end
    
    -- Run each dstream poll() function, until it returns
    for _, dstream in ipairs(self.dstreams) do
      local rdds = dstream:poll(individualDStreamDurationBudget)
      if rdds ~= nil and #rdds > 0 then
        for _, rdd in ipairs(rdds) do dstream:_notify(currentTime, rdd) end
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
  local moses = require 'moses'
  if not moses.isBoolean(oneAtATime) then oneAtATime = true end
  local RDD = require 'stuart.RDD'
  rdds = moses.map(rdds, function(rdd)
    if not class.istype(rdd, RDD) then rdd = self.sc:makeRDD(rdd) end
    return rdd
  end)
  local QueueInputDStream = require 'stuart.streaming.QueueInputDStream'
  local dstream = QueueInputDStream.new(self, rdds, oneAtATime)
  self.dstreams[#self.dstreams+1] = dstream
  return dstream
end

function StreamingContext:receiverStream(receiver)
  local ReceiverInputDStream = require 'stuart.streaming.ReceiverInputDStream'
  local dstream = ReceiverInputDStream.new(self, receiver)
  self.dstreams[#self.dstreams+1] = dstream
  return dstream
end

function StreamingContext:socketTextStream(hostname, port)
  local SocketInputDStream = require 'stuart.streaming.SocketInputDStream'
  local dstream = SocketInputDStream.new(self, hostname, port)
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
