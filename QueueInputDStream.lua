local DStream = require 'DStream'

local QueueInputDStream = DStream:new()

function QueueInputDStream:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function QueueInputDStream:compute(durationBudget)
  while true do
    -- one at a time
    --local rdd = table.remove(self.queue)
    --coroutine.yield({rdd})

    local rdds = self.queue
    self.queue = {}
    coroutine.yield(rdds)
  end
end

return QueueInputDStream
