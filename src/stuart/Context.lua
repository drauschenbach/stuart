local class = require 'stuart.class'

local Context = class.new()

function Context:_init(arg1, arg2, arg3, arg4)
  local SparkConf = require 'stuart.SparkConf'
  if arg1 == nil and arg2 == nil then
    self.conf = SparkConf.new()
  elseif class.istype(arg1, SparkConf) then
    self.conf = arg1
  else
    self.conf = Context._updatedConf(SparkConf.new(), arg1, arg2, arg3, arg4)
  end
  
  self.defaultParallelism = 1
  self.defaultMinPartitions = 1
  self.lastRddId = 0
  self.stopped = false
  local logging = require 'stuart.internal.logging'
  logging.logInfo('Running Stuart (Embedded Spark 2.2)')
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
  local fileSystemFactory = require 'stuart.fileSystemFactory'
  local fs, openPath = fileSystemFactory.createForOpenPath(path)
  if fs:isDirectory(openPath) then
    local fileStatuses = fs:listStatus(openPath)
    local lines = {}
    for _,fileStatus in ipairs(fileStatuses) do
      if fileStatus.type == 'FILE' and fileStatus.pathSuffix:sub(1,1) ~= '.' and fileStatus.pathSuffix:sub(1,1) ~= '_' then
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
  local moses = require 'moses'
  if not moses.isNumber(numPartitions) then numPartitions = self.defaultParallelism end
  local Partition = require 'stuart.Partition'
  local RDD = require 'stuart.RDD'
  if numPartitions == 1 then
    local p = Partition.new(x, 0)
    return RDD.new(self, {p})
  end
  
  local chunks = {}
  local chunkSize = math.ceil(#x / numPartitions)
  if chunkSize > 0 then
    chunks = moses.tabulate(moses.partition(x, chunkSize))
  end
  while #chunks < numPartitions do chunks[#chunks+1] = {} end -- pad-right empty partitions
  local partitions = moses.map(chunks, function(chunk, i)
    return Partition.new(chunk, i)
  end)
  return RDD.new(self, partitions)
end

function Context:setLogLevel(level)
  local logging = require 'stuart.internal.logging'
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
