local _ = require 'lodash'
local DStream = require 'DStream'
local inspect = require 'inspect'
local QueueInputDStream = require 'QueueInputDStream'
local socket = require 'socket'

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
  if not _.isNumber(timeout) or timeout <= 0 then error('Invalid timeout') end
  
  -- run loop
  local startTime = socket.gettime()
  while true do
    local now = socket.gettime()
    local elapsedTime = now - startTime
    if elapsedTime > timeout then break end
    for i, dstream in ipairs(self.dstreams) do
      rdd = dstream:compute(now)
      if not _.isNil(rdd) then
        dstream:_notify(rdd)
      end
    end
    sleep(.1)
  end
  --_.print('Ending run loop')
end

function StreamingContext:getState()
  return self.state
end

function StreamingContext:queueStream(rdds, oneAtATime)
  if not _.isBoolean(oneAtATime) then oneAtATime = true end
  dstream = QueueInputDStream:new({sc=self.sc, batchDuration=self.batchDuration, queue=rdds})
  table.insert(self.dstreams, dstream)
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
