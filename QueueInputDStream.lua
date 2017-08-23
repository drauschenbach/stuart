local class = require 'middleclass'
local DStream = require 'DStream'

local QueueInputDStream = class('QueueInputDStream', DStream)

function QueueInputDStream:initialize(ctx, rdds)
  DStream.initialize(self, ctx)
  self.queue = rdds
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
