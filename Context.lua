local moses = require 'moses'
local Partition = require 'Partition'
local RDD = require 'RDD'

Context = {master='local[1]', appName=nil}

function Context:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function Context:makeRDD(x, numPartitions)
  return self:parallelize(x, numPartitions)
end

function Context:parallelize(x, numPartitions)
  if numPartitions == 1 or not moses.isNumber(numPartitions) then
    local p = Partition:new({}, x, 0)
    return RDD:new(nil, {p}, self)
  end
  
  local chunks = moses.array(moses.partition(x, math.ceil(#x / numPartitions)))
  local partitions = moses.map(chunks, function(i, chunk)
    return Partition:new({}, chunk, i)
  end)
	return RDD:new(nil, partitions, self)
end

function Context:textFile(filename)
  local f = assert(io.open(filename, 'r'))
  local content = f:read '*all'
  f:close()
  local lines = {}
  for line in content:gmatch('[^\r\n]+') do
    lines[#lines+1] = line
  end
  return self:parallelize(lines)
end

function Context:union(rdds)
  local t = rdds[1]
  for i = 2, #rdds do t = t:union(rdds[i]) end
  return t
end

return Context
