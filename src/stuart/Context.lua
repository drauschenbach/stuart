local class = require 'middleclass'
local fileSystemFactory = require 'stuart.fileSystemFactory'
local isInstanceOf = require 'stuart.util.isInstanceOf'
local logging = require 'stuart.internal.logging'
local moses = require 'stuart.util.moses'
local Partition = require 'stuart.Partition'
local RDD = require 'stuart.RDD'
local SparkConf = require 'stuart.SparkConf'

local Context = class('Context')
Context.SPARK_VERSION = '2.2.0'

function Context:initialize(arg1, arg2, arg3, arg4)
  if arg1 == nil and arg2 == nil then
    self.conf = SparkConf:new()
  elseif isInstanceOf(arg1, SparkConf) then
    self.conf = arg1
  else
    self.conf = Context._updatedConf(SparkConf:new(), arg1, arg2, arg3, arg4)
  end
  
  self.defaultParallelism = 1
  self.lastRddId = 0
  self.stopped = false
  logging.logInfo('Running Stuart (Embedded Spark) version ' .. Context.SPARK_VERSION)
end

function Context:appName()
  return self.conf:get('spark.app.name')
end

function Context:emptyRDD()
  local rdd = self:parallelize({}, 0)
  return rdd
end

function Context:getConf()
  return self.conf:clone()
end

function Context:getNextId()
  self.lastRddId = self.lastRddId + 1
  return self.lastRddId
end

function Context:hadoopFile(path, minPartitions)
  local fs, openPath = fileSystemFactory.createForOpenPath(path)
  if fs:isDirectory(openPath) then
    local fileStatuses = fs:listStatus(openPath)
    local lines = {}
    for _,fileStatus in ipairs(fileStatuses) do
      if fileStatus.pathSuffix:sub(1,1) ~= '.' and fileStatus.pathSuffix:sub(1,1) ~= '_' then
        local uri = openPath .. '/' .. fileStatus.pathSuffix
        local content, status = fs:open(uri)
        if status and status >= 400 then error(content) end
        for line in content:gmatch('[^\r\n]+') do
          lines[#lines+1] = line
        end
      end
    end
    return self:parallelize(lines, minPartitions)
  else
    local content = fs:open(openPath)
    local lines = {}
    for line in content:gmatch('[^\r\n]+') do
      lines[#lines+1] = line
    end
    return self:parallelize(lines, minPartitions)
  end
end

function Context:isStopped()
  return self.stopped
end

function Context:makeRDD(x, numPartitions)
  return self:parallelize(x, numPartitions)
end

function Context:master()
  return self.conf:get('spark.master')
end

function Context:parallelize(x, numPartitions)
  assert(not self.stopped)
  if not moses.isNumber(numPartitions) then numPartitions = self.defaultParallelism end
  if numPartitions == 1 then
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

function Context:setLogLevel(level)
  logging.log:setLevel(level)
end

function Context:stop()
  self.stopped = true
end

function Context:textFile(path, minPartitions)
  assert(not self.stopped)
  return self:hadoopFile(path, minPartitions)
end

function Context:union(rdds)
  local t = rdds[1]
  for i = 2, #rdds do t = t:union(rdds[i]) end
  return t
end

function Context._updatedConf(conf, master, appName, sparkHome)
  local res = conf:clone()
  res:setMaster(master)
  res:setAppName(appName)
  if sparkHome ~= nil then
    res:setSparkHome(sparkHome)
  end
  return res
end

return Context
