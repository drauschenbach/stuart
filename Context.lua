local class = require 'middleclass'
local moses = require 'moses'
local Partition = require 'Partition'
local RDD = require 'RDD'

local Context = class('Context')

function Context:initialize(master, appName)
  self.nextRddId = 1
  self.master = master or 'local[1]'
  self.appName = appName
end

function Context:emptyRDD()
  local rdd = self:parallelize({}, 0)
  return rdd
end

function Context:getNextId()
  self.nextRddId = self.nextRddId + 1
  return self.nextRddId - 1 
end

function Context:makeRDD(x, numPartitions)
  return self:parallelize(x, numPartitions)
end

function Context:parallelize(x, numPartitions)
  if numPartitions == 1 or not moses.isNumber(numPartitions) then
    local p = Partition:new(x, 0)
    return RDD:new(self, {p})
  end
  
  local chunks = {}
  local chunkSize = math.ceil(#x / numPartitions)
  if chunkSize > 0 then
    chunks = moses.array(moses.partition(x, chunkSize))
  end
  while #chunks < numPartitions do chunks[#chunks+1] = {} end -- pad-right empty partitions
  local partitions = moses.map(chunks, function(i, chunk)
    return Partition:new(chunk, i)
  end)
	return RDD:new(self, partitions)
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
