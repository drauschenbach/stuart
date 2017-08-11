DStream = {ctx=nil, callbacks={}}

function DStream:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function DStream:_notify(rdd)
  for i, cb in ipairs(self.callbacks) do
    cb(rdd)
  end
end

function DStream:foreachRDD(f)
  table.insert(self.callbacks, f)
end

return DStream
