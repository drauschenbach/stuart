local _ = require 'lodash'
_.groupBy = require 'lodashPatchedGroupBy'
local Partition = require 'Partition'
local RDD = require 'RDD'

Context = {}

function Context:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function Context:parallelize(x, numPartitions)
  if numPartitions == 1 or not _.isNumber(numPartitions) then
    local p = Partition:new({}, x, 0)
    return RDD:new(nil, {p}, self)
  end
  
  local chunks = _.chunk(x, #x / numPartitions)
  local partitions = _.map(chunks, function(chunk, i)
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

return Context
