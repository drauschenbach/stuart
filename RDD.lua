local moses = require 'moses'

RDD = {partitions={}, ctx=nil}

function RDD:new(o)
  o = o or {}
  setmetatable(o, self)
  self.__index = self
  return o
end

function RDD:_dict()
  return moses.reduce(self:collect(), function(r, e)
    r[e[1]] = e[2]
    return r
  end, {})
end

function RDD:_flatten()
  for i, p in ipairs(self.partitions) do p:_flatten() end
  return self
end

function RDD:_flattenValues()
  for i, p in ipairs(self.partitions) do p:_flattenValues() end
  return self
end

function RDD:aggregate(zeroValue, seqOp, combOp)
  return moses.reduce(self.partitions, function(r, p)
    local y = moses.reduce(p.x, seqOp, moses.clone(zeroValue))
    return combOp(r, y)
  end, moses.clone(zeroValue))
end

function RDD:cache()
  return self
end

function RDD:cartesian(other)
  local t = {}
  moses.forEach(self:collect(), function(i,x)
    moses.forEach(other:collect(), function(j,y)
      t[#t+1] = {x, y}
    end)
  end)
  return self.ctx:parallelize(t)
end

function RDD:coalesce()
  return self
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

function RDD:context()
  return self.ctx
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
  return moses.reduce(self:collect(), function(r, n, key)
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
  return self.ctx:parallelize(t, numPartitions)
end

function RDD:filter(f)
  local t = moses.filter(self:collect(), function(k,v) return f(v) end)
  return self.ctx:parallelize(t)
end

function RDD:first()
  return self.partitions[1].x[1]
end

function RDD:flatMap(f, preservesPartitioning)
  return self:map(f):_flatten()
end

function RDD:flatMapValues(f)
  return self:mapValues(f):_flattenValues()
end

function RDD:fold(zeroValue, op)
  return moses.reduce(self:collect(), op, zeroValue)
end

function RDD:foldByKey(zeroValue, op)
  local keys = moses.unique(moses.map(self:collect(), function(i,e) return e[1] end))
  local t = moses.map(keys, function(i,k)
    c = moses.map(self:collect(), function(j,e)
      if e[1] == k then return e[2] end
    end)
    return {k, moses.reduce(c, op, zeroValue)}
  end)
  return self.ctx:parallelize(t)
end

function RDD:foreach(f)
  for z, p in ipairs(self.partitions) do
    for i, x in ipairs(p.x) do
      p.x[i] = f(p.x[i])
    end
  end
end

function RDD:foreachPartition(f)
  for z, p in ipairs(self.partitions) do
    f(p.x)
  end
end

function RDD:glom(f)
  local t = moses.map(self.partitions, function(k,p) return p.x end)
  return self.ctx:parallelize(t)
end

function RDD:groupBy(f)
  local x = self:collect()
  local keys = moses.unique(moses.map(x, function(k,v) return f(v) end))
  local t = moses.map(keys, function(i,k)
    v = moses.reduce(x, function(r, e)
      if f(e) == k then r[#r+1] = e end
      return r
    end, {})
    return {k, v}
  end)
  return self.ctx:parallelize(t)
end

function RDD:groupByKey()
  local keys = moses.keys(self:_dict())
  local t = moses.map(keys, function(i,k)
    v = moses.reduce(self:collect(), function(r, e)
      if e[1] == k then r[#r+1] = e[2] end
      return r
    end, {})
    return {k, v}
  end)
  return self.ctx:parallelize(t)
end

function RDD:histogram(buckets)
  if moses.isNumber(buckets) then
    local num_buckets = buckets
    local min_v = self:min()
    local max_v = self:max()
    buckets = moses.map(moses.range(0, num_buckets), function(i,v)
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
  moses.forEach(self:collect(), function(i,x)
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

function RDD:id()
  error('NIY')
end

function RDD:intersection(other)
  return self.ctx:parallelize(moses.intersection(moses.unique(self:collect()), moses.unique(other:collect())))
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
    moses.forEach(self:collect(), function(i,x)
      if x[1] == key then
        moses.forEach(other:collect(), function(j,y)
          if y[1] == key then
            r[#r+1] = {key, {x[2], y[2]}}
          end
        end)
      end
    end)
    return r
  end, {})
  return self.ctx:parallelize(t)
end

function RDD:keyBy(f)
  local t = moses.map(self:collect(), function(i,e) return {f(e), e} end)
  return self.ctx:parallelize(t)
end

function RDD:keys()
  local t = moses.map(self:collect(), function(i,e) return e[1] end)
  return self.ctx:parallelize(t)
end

function RDD:leftOuterJoin(other)
  local d1 = self:_dict()
  local d2 = other:_dict()
  local t = moses.reduce(self:collect(), function(r, e)
      local right = {}
      moses.forEach(other:collect(), function(i,y)
        if y[1] == e[1] then right[#right+1] = y[2] end
      end)
      if #right == 0 then
        r[#r+1] = {e[1], {e[2], nil}}
      else
        moses.forEach(right, function(i,z)
          r[#r+1] = {e[1], {e[2], z}}
        end)
      end
    return r
  end, {})
  return self.ctx:parallelize(t)
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
  return self.ctx:parallelize(t)
end

function RDD:mapPartitions(iter)
  local t = moses.reduce(self.partitions, function(r,p)
    for e in iter(p:_toLocalIterator()) do
      r[#r+1] = e
    end
    return r
  end, {})
  return self.ctx:parallelize(t)
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
  return self.ctx:parallelize(t)
end

function RDD:mapValues(f)
  local t = moses.map(self:collect(), function(i,e) return {e[1], f(e[2])} end)
  return self.ctx:parallelize(t)
end

function RDD:max()
  local r = self:first()
  for n in self:toLocalIterator() do
    if n > r then r = n end
  end
  return r
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
  return self.ctx:parallelize(self:collect(), numPartitions)
end

function RDD:rightOuterJoin(other)
  local t = moses.reduce(other:collect(), function(r, e)
      local left = {}
      moses.forEach(self:collect(), function(i,y)
        if y[1] == e[1] then left[#left+1] = y[2] end
      end)
      if #left == 0 then
        r[#r+1] = {e[1], {nil, e[2]}}
      else
        moses.forEach(left, function(i,z)
          r[#r+1] = {e[1], {z, e[2]}}
        end)
      end
    return r
  end, {})
  return self.ctx:parallelize(t)
end

function RDD:stats()
  local x = self:collect()
  local r = moses.reduce(x, function(r, v)
    r.count = r.count + 1
    r.sum = r.sum + v
    return r
  end, {count=0, sum=0})
  r.mean = r.sum / r.count
  
  local sumOfSquares = moses.reduce(x, function(r, v) return r + v*v end, 0)
  r.stdev = math.sqrt((r.count * sumOfSquares - r.sum * r.sum) / (r.count * (r.count-1)))
  r.sum = nil
  return r
end

function RDD:subtract(other)
  local t = moses.without(self:collect(), other:collect())
  return self.ctx:parallelize(t, #self.partitions)
end

function RDD:subtractByKey(other)
  local selfKeys = self:keys():collect()
  local otherKeys = other:keys():collect()
  local keys = moses.without(selfKeys, otherKeys)
  local t = moses.reduce(self:collect(), function(r, e)
    if moses.detect(keys, e[1]) ~= nil then r[#r+1] = e end
    return r 
  end, {})
  return self.ctx:parallelize(t, #self.partitions)
end

function RDD:take(n)
  local iter = self:toLocalIterator()
  t = {}
  for i = 1, n, 1 do
    t[#t+1] = iter()
  end
  return t
end

function RDD:takeSample(withReplacement, num, seed)
  return moses.sample(self:collect(), num)
end

function RDD:toLocalIterator()
  local pIndex = 1
  local i = 0
  return function()
    if pIndex > #self.partitions then return nil end
    local partitionData = self.partitions[pIndex].x
    if not moses.isTable(partitionData) then return nil end
    i = i + 1
    if i > #partitionData then
      pIndex = pIndex + 1
      i = 1
      if pIndex > #self.partitions then return nil end
      partitionData = self.partitions[pIndex].x
    end
    
    if pIndex <= #self.partitions and i <= #partitionData then
      return partitionData[i]
    end
  end
end

function RDD:union(other)
  local t = moses.append(self:collect(), other:collect())
  return self.ctx:parallelize(t)
end

function RDD:values(f)
  local t = moses.map(self:collect(), function(k,e) return e[2] end)
  return self.ctx:parallelize(t)
end

function RDD:zip(other)
  local t = moses.zip(self:collect(), other:collect())
  return self.ctx:parallelize(t)
end

return RDD
