DStream = {ctx=nil, inputs={}, outputs={}}

function DStream:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

--------------------------------------------------------------------------------

local TransformedDStream = DStream:new()

function TransformedDStream:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function TransformedDStream:_notify(validTime, rdd)
  local t = self.transformFunc(rdd)
  return t
end

--------------------------------------------------------------------------------

function DStream:_notify(validTime, rdd)
  for i, dstream in ipairs(self.inputs or {}) do
    rdd = dstream:_notify(validTime, rdd)
  end
  for i, dstream in ipairs(self.outputs or {}) do
    dstream:_notify(validTime, rdd)
  end
end

function DStream:foreachRDD(foreachFunc)
  local dstream = TransformedDStream:new{ctx=self.ctx, transformFunc=foreachFunc}
  self.outputs[#self.outputs+1] = dstream
  return self
end

function DStream:transform(transformFunc)
  local dstream = TransformedDStream:new{ctx=self.ctx, transformFunc=transformFunc}
  self.inputs[#self.inputs+1] = dstream
  return dstream
end

return DStream
