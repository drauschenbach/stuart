local class = require 'middleclass'
local log = require 'stuart.internal.logging'.log
local moses = require 'stuart.util.moses'

local RDD = class('RDD')

function RDD:initialize(context, partitions)
  self.context = context
  self.id = context:getNextId()
  getmetatable(self).sparkContext = context
  self.partitions = partitions
end

function RDD:_dict()
  return moses.reduce(self:collect(), function(r, e)
    r[e[1]] = e[2]
    return r
  end, {})
end

function RDD:_flatten()
  for _, p in ipairs(self.partitions) do p:_flatten() end
  return self
end

function RDD:_flattenValues()
  for _, p in ipairs(self.partitions) do p:_flattenValues() end
  return self
end

function RDD:__tostring()
  return 'RDD[' .. self.id .. ']'
end

function RDD:aggregate(zeroValue, seqOp, combOp)
  return moses.reduce(self.partitions, function(r, p)
    local y = moses.reduce(p.data, seqOp, moses.clone(zeroValue))
    return combOp(r, y)
  end, moses.clone(zeroValue))
end

function RDD:aggregateByKey(zeroValue, seqOp, combOp)
  local y = moses.map(self.partitions, function(_,p)
    local keys = moses.uniq(moses.map(p.data, function(_,e) return e[1] end))
    local z = moses.reduce(keys, function(r,key)
      local valuesForKey = moses.reduce(p.data, function(r2,e)
        if e[1] == key then r2[#r2+1] = e[2] end
        return r2
      end, {})
      r[key] = moses.reduce(valuesForKey, seqOp, zeroValue)
      return r
    end, {})
    return z
  end, zeroValue)
  
  local keys = moses.uniq(moses.reduce(y, function(r,e) return moses.append(r, moses.keys(e)) end, {}))
  local t = moses.reduce(keys, function(r,key)
    local valuesForKey = moses.reduce(y, function(r2,e)
      for k,v in pairs(e) do
        if k == key then r2[#r2+1] = v end
      end
      return r2
    end, {})
    r[#r+1] = {key, moses.reduce(valuesForKey, combOp, 0)}
    return r
  end, {})
  return self.context:parallelize(t)
end

function RDD:cache()
  return self
end

function RDD:cartesian(other)
  local t = {}
  moses.forEach(self:collect(), function(_,x)
    moses.forEach(other:collect(), function(_,y)
      t[#t+1] = {x, y}
    end)
  end)
  return self.context:parallelize(t)
end

function RDD:coalesce(numPartitions, shuffle)
  if not moses.isBoolean(shuffle) then shuffle = false end
  return self.context:parallelize(self:collect(), numPartitions)
end

function RDD:collect(f)
  local t = moses.array(self:toLocalIterator())
  if moses.isFunction(f) then
    -- reduce, not map, because Lua arrays cannot contain nil
    t = moses.reduce(t, function(r, v)
      local x = f(v)
      if x ~= nil then r[#r+1] = x end
      return r
    end, {})
  end
  return t
end

function RDD:collectAsMap()
  local t = moses.array(self:toLocalIterator())
  -- now ensure keys are unique, since we are observing the Java Map (non-multimap) contract
  t = moses.reduce(t, function(r, v)
    r[v[1]] = v[2]
    return r
  end, {})
  return t
end

function RDD:combineByKey(createCombiner, mergeValue, mergeCombiners)
  assert(moses.isFunction(createCombiner))
  assert(moses.isFunction(mergeValue))
  assert(moses.isFunction(mergeCombiners))
  local y = moses.map(self.partitions, function(_,p)
    local keys = moses.uniq(moses.map(p.data, function(_,e) return e[1] end))
    local z = moses.reduce(keys, function(r,key)
      local valuesForKey = moses.reduce(p.data, function(r2,e)
        if e[1] == key then r2[#r2+1] = e[2] end
        return r2
      end, {})
      r[key] = moses.reduce(valuesForKey, mergeValue, {})
      return r
    end, {})
    return z
  end)
  
  local keys = moses.uniq(moses.reduce(y, function(r,e) return moses.append(r, moses.keys(e)) end, {}))
  local t = moses.reduce(keys, function(r,key)
    local valuesForKey = moses.reduce(y, function(r2,e)
      for k,v in pairs(e) do
        if k == key then r2[#r2+1] = v end
      end
      return r2
    end, {})
    r[#r+1] = {key, moses.reduce(valuesForKey, mergeCombiners, {})}
    return r
  end, {})
  return self.context:parallelize(t)
end

function RDD:count()
  return moses.reduce(self.partitions, function(r, p) return r + p:_count() end, 0)
end

function RDD:countApprox()
  return self:count()
end

function RDD:countByKey()
  return moses.reduce(self:collect(), function(r, e)
    local k = e[1]
    if r[k] == nil then
      r[k] = 1
    else
      r[k] = r[k] + 1
    end
    return r
  end, {})
end

function RDD:countByValue()
  return moses.reduce(self:collect(), function(r, n)
    if r[n] == nil then
      r[n] = 1
    else
      r[n] = r[n] + 1
    end
    return r
  end, {})
end

function RDD:distinct(numPartitions)
  local t = moses.uniq(self:collect())
  return self.context:parallelize(t, numPartitions)
end

function RDD:filter(f)
  local t = moses.filter(self:collect(), function(_,v) return f(v) end)
  return self.context:parallelize(t)
end

function RDD:filterByRange(lower, upper)
  local f = function(x)
    if moses.isTable(x) then return x[1] >= lower and x[1] <= upper end
    return false
  end
  return self:filter(f)
end

function RDD:first()
  return self.partitions[1].data[1]
end

function RDD:flatMap(f)
  return self:map(f):_flatten()
end

function RDD:flatMapValues(f)
  return self:mapValues(f):_flattenValues()
end

function RDD:fold(zeroValue, op)
  return moses.reduce(self:collect(), op, zeroValue)
end

function RDD:foldByKey(zeroValue, op)
  local keys = moses.unique(moses.map(self:collect(), function(_,e) return e[1] end))
  local t = moses.map(keys, function(_,k)
    local c = moses.map(self:collect(), function(_,e)
      if e[1] == k then return e[2] end
    end)
    return {k, moses.reduce(c, op, zeroValue)}
  end)
  return self.context:parallelize(t)
end

function RDD:foreach(f)
  for _, p in ipairs(self.partitions) do
    for i, _ in ipairs(p.data) do
      p.data[i] = f(p.data[i])
    end
  end
end

function RDD:foreachPartition(f)
  for _, p in ipairs(self.partitions) do
    f(p.data)
  end
end

function RDD:glom()
  local t = moses.map(self.partitions, function(_,p) return p.data end)
  return self.context:parallelize(t)
end

function RDD:groupBy(f)
  local x = self:collect()
  local keys = moses.unique(moses.map(x, function(_,v) return f(v) end))
  local t = moses.map(keys, function(_,k)
    local v = moses.reduce(x, function(r, e)
      if f(e) == k then r[#r+1] = e end
      return r
    end, {})
    return {k, v}
  end)
  return self.context:parallelize(t)
end

function RDD:groupByKey(numPartitions)
  numPartitions = numPartitions or #self.partitions
  local keys = moses.keys(self:_dict())
  local t = moses.map(keys, function(_,k)
    local v = moses.reduce(self:collect(), function(r, e)
      if e[1] == k then r[#r+1] = e[2] end
      return r
    end, {})
    return {k, v}
  end)
  return self.context:parallelize(t, numPartitions)
end

function RDD:histogram(buckets)
  if moses.isNumber(buckets) then
    local num_buckets = buckets
    local min_v = self:min()
    local max_v = self:max()
    buckets = moses.map(moses.range(0, num_buckets), function(_,v)
      return min_v + v*(max_v-min_v)/num_buckets
    end)
    local h = self:_histogram(buckets)
    return buckets, h
  end
  local h = self:_histogram(buckets)
  return h
end

function RDD:_histogram(buckets)
  local num_buckets = #buckets - 1
  local h = {}; moses.fill(h, 0, 1, num_buckets)
  moses.forEach(self:collect(), function(_,x)
    for i = 1, num_buckets, 1 do
      local shouldAdd
      local lastBucket = i == num_buckets
      if lastBucket then -- last bucket is inclusive
        shouldAdd = x >= buckets[i] and x <= buckets[i+1]
      else
        shouldAdd = x >= buckets[i] and x < buckets[i+1]
      end
      if shouldAdd then h[i] = h[i] + 1 end
    end
  end)
  return h
end

function RDD:intersection(other)
  return self.context:parallelize(moses.intersection(moses.unique(self:collect()), moses.unique(other:collect())))
end

function RDD:isCheckpointed()
  return false
end

function RDD:isEmpty()
  return self:count() <= 0
end

function RDD:join(other)
  local keys = moses.intersection(moses.keys(self:_dict()), moses.keys(other:_dict()))
  local t = moses.reduce(keys, function(r, key)
    moses.forEach(self:collect(), function(_,x)
      if x[1] == key then
        moses.forEach(other:collect(), function(_,y)
          if y[1] == key then
            r[#r+1] = {key, {x[2], y[2]}}
          end
        end)
      end
    end)
    return r
  end, {})
  return self.context:parallelize(t, math.max(#self.partitions, #other.partitions))
end

function RDD:keyBy(f)
  local t = moses.map(self:collect(), function(_,e) return {f(e), e} end)
  return self.context:parallelize(t)
end

function RDD:keys()
  local t = moses.map(self:collect(), function(_,e) return e[1] end)
  return self.context:parallelize(t)
end

function RDD:leftOuterJoin(other)
  --local d1 = self:_dict()
  --local d2 = other:_dict()
  local t = moses.reduce(self:collect(), function(r, e)
      local right = {}
      moses.forEach(other:collect(), function(_,y)
        if y[1] == e[1] then right[#right+1] = y[2] end
      end)
      if #right == 0 then
        r[#r+1] = {e[1], {e[2], nil}}
      else
        moses.forEach(right, function(_,z)
          r[#r+1] = {e[1], {e[2], z}}
        end)
      end
    return r
  end, {})
  return self.context:parallelize(t)
end

function RDD:lookup(key)
  return moses.reduce(self:collect(), function(r, e)
    if e[1] == key then r[#r+1] = e[2] end
    return r
  end, {})
end

function RDD:map(f)
  local t = {}
  for e in self:toLocalIterator() do t[#t+1] = f(e) end
  return self.context:parallelize(t, #self.partitions)
end

function RDD:mapPartitions(iter)
  local t = moses.reduce(self.partitions, function(r,p)
    for e in iter(p:_toLocalIterator()) do
      r[#r+1] = e
    end
    return r
  end, {})
  return self.context:parallelize(t)
end

function RDD:mapPartitionsWithIndex(iter)
  local index = 0
  local t = moses.reduce(self.partitions, function(r,p)
    for e in iter(index, p:_toLocalIterator()) do
      r[#r+1] = e
    end
    index = index + 1
    return r
  end, {})
  return self.context:parallelize(t)
end

function RDD:mapValues(f)
  local t = moses.map(self:collect(), function(_,e) return {e[1], f(e[2])} end)
  return self.context:parallelize(t)
end

function RDD:max()
  local r = self:first()
  for n in self:toLocalIterator() do
    if n > r then r = n end
  end
  return r
end

function RDD:mean()
  return self:stats().mean
end

function RDD:meanApprox()
  return self:mean()
end

function RDD:min()
  local r = self:first()
  for n in self:toLocalIterator() do
    if n < r then r = n end
  end
  return r
end

function RDD:reduce(f)
  return moses.reduce(self:collect(), f)
end

function RDD:reduceByKey(f)
  return self:groupByKey():mapValues(function(e) return moses.reduce(e, f) end)
end

function RDD:repartition(numPartitions)
  return self:coalesce(numPartitions, true)
end

function RDD:rightOuterJoin(other)
  local t = moses.reduce(other:collect(), function(r, e)
      local left = {}
      moses.forEach(self:collect(), function(_,y)
        if y[1] == e[1] then left[#left+1] = y[2] end
      end)
      if #left == 0 then
        r[#r+1] = {e[1], {nil, e[2]}}
      else
        moses.forEach(left, function(_,z)
          r[#r+1] = {e[1], {z, e[2]}}
        end)
      end
    return r
  end, {})
  return self.context:parallelize(t)
end

function RDD:sample(_, fraction, seed)
  assert(fraction >= 0, 'Fraction must be nonnegative, but got ' .. fraction)
  local t = self:collect()
  local n = math.max(1, #t * fraction)
  if n > #t then return {} end
  local r = moses.sample(self:collect(), n, seed)
  return self.context:parallelize(r)
end

function RDD:setName(name)
  self.name = name
end

function RDD:sortBy(f, ascending, numPartitions)
  if not moses.isBoolean(ascending) then ascending = true end
  local t = self:collect()
  local comp
  if ascending then
    comp = function(a,b) return a<b end
  else
    comp = function(a,b) return a>b end
  end
  t = moses.sortBy(t, f, comp)
  return self.context:parallelize(t, numPartitions)
end

function RDD:sortByKey(ascending, numPartitions)
  if not moses.isBoolean(ascending) then ascending = true end
  if not moses.isNumber(numPartitions) then numPartitions = #self.partitions end
  local f = function(a,b)
    if not moses.isTable(a) or not moses.isTable(b) then return 0 end
    if ascending then
      return a[1] < b[1]
    else
      return a[1] > b[1]
    end
  end
  local t = moses.sort(self:collect(), f)
  return self.context:parallelize(t, numPartitions)
end

function RDD:stats()
  local x = self:collect()
  local r = moses.reduce(x, function(r, v)
    r.count = r.count + 1
    r.sum = r.sum + v
    return r
  end, {count=0, sum=0})
  r.mean = r.sum / r.count
  
  local sumOfSquares = moses.reduce(x, function(acc, v) return acc + v*v end, 0)
  r.stdev = math.sqrt((r.count * sumOfSquares - r.sum * r.sum) / (r.count * (r.count-1)))
  r.sum = nil
  return r
end

function RDD:stdev()
  local m = self:stats().mean
  local vm
  local sum = 0
  local count = 0
  for _,v in pairs(self:collect()) do
    vm = v - m
    sum = sum + vm * vm
    count = count + 1
  end
  local result = math.sqrt(sum / (count-1))
  return result
end

function RDD:subtract(other)
  local t = moses.without(self:collect(), other:collect())
  return self.context:parallelize(t, #self.partitions)
end

function RDD:subtractByKey(other)
  local selfKeys = self:keys():collect()
  local otherKeys = other:keys():collect()
  local keys = moses.without(selfKeys, otherKeys)
  local t = moses.reduce(self:collect(), function(r, e)
    if moses.detect(keys, e[1]) ~= nil then r[#r+1] = e end
    return r
  end, {})
  return self.context:parallelize(t, #self.partitions)
end

function RDD:sum()
  return moses.reduce(self:collect(), function(r, v) return r+v end, 0)
end

function RDD:sumApprox()
  return self:sum()
end

function RDD:take(n)
  local iter = self:toLocalIterator()
  local t = {}
  for i = 1, n, 1 do
    local x = iter()
    if x == nil then break end
    t[#t+1] = x
  end
  return t
end

function RDD:takeSample(withReplacement, num, seed)
  assert(num >= 0, 'Negative number of elements requested')

  if num == 0 then return {} end
  local initialCount = self:count()
  if initialCount == 0 then return {} end
  
  if seed ~= nil then math.randomseed(seed) end

  local randomizeInPlace = require 'stuart.util.spark.randomizeInPlace'
  if not withReplacement and num >= initialCount then
    return randomizeInPlace(self:collect())
  end
  
  local samplingUtils = require 'stuart.util.spark.samplingUtils'
  local fraction = samplingUtils.computeFractionForSampleSize(num, initialCount, withReplacement)
  local samples = self:sample(withReplacement, fraction, math.random(32000)):collect()

  -- If the first sample didn't turn out large enough, keep trying to take samples;
  -- this shouldn't happen often because we use a big multiplier for the initial size
  local numIters = 0
  while #samples < num do
    log:warn('Needed to re-sample due to insufficient sample size. Repeat #' .. numIters)
    samples = self:sample(withReplacement, fraction, math.random(32000)):collect()
    numIters = numIters + 1
  end
  return moses.first(randomizeInPlace(samples), num)
end

function RDD:toLocalIterator()
  local pIndex = 1
  local i = 0
  return function()
    if pIndex > #self.partitions then return nil end
    local partitionData = self.partitions[pIndex].data
    if not moses.isTable(partitionData) then return nil end
    i = i + 1
    if i > #partitionData then
      pIndex = pIndex + 1
      i = 1
      if pIndex > #self.partitions then return nil end
      partitionData = self.partitions[pIndex].data
    end
    
    if pIndex <= #self.partitions and i <= #partitionData then
      return partitionData[i]
    end
  end
end

function RDD:top(num)
  local t = moses.sort(self:collect(), function(a,b) return a>b end)
  return moses.slice(t, 1, num)
end

function RDD:toString()
  return tostring(self)
end

function RDD:union(other)
  local t = moses.append(self:collect(), other:collect())
  return self.context:parallelize(t)
end

function RDD:values()
  local t = moses.map(self:collect(), function(_,e) return e[2] end)
  return self.context:parallelize(t)
end

function RDD:zip(other)
  local t = moses.zip(self:collect(), other:collect())
  return self.context:parallelize(t)
end

function RDD:zipWithIndex()
  local t = moses.map(self:collect(), function(i,x)
    return {x,i-1}
  end)
  return self.context:parallelize(t)
end

return RDD
