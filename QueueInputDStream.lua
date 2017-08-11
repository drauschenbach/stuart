local _ = require 'lodash'
local DStream = require 'DStream'

local QueueInputDStream = DStream:new()

function QueueInputDStream:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function QueueInputDStream:compute(time)
  return table.remove(self.queue)
end

return QueueInputDStream
