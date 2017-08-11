local _ = require 'lodash'
_.groupBy = require 'lodashPatchedGroupBy'
local inspect = require 'inspect'

RDD = {}

function RDD:new(o, partitions, ctx)
  o = o or {}
  o.p = partitions
  o.ctx = ctx
  setmetatable(o, self)
  self.__index = self
  return o
end

function RDD:_dict()
  return _.reduce(self:collect(), function(r, e)
    r[e[1]] = e[2]
    return r
  end, {})
end

function RDD:_flatten()
  for i, p in ipairs(self.p) do p:_flatten() end
  return self
end

function RDD:_flattenValues()
  for i, p in ipairs(self.p) do p:_flattenValues() end
  return self
end

function _iterToArray(iter)
  local r = {}
  for e in iter do table.insert(r, e) end
  return r
end

function RDD:aggregate(zeroValue, seqOp, combOp)
  return _.reduce(self.p, function(r, p)
    local y = _.reduce(p.x, seqOp, zeroValue)
    return combOp(r, y)
  end, zeroValue)
end

function RDD:cache()
  error('NIY')
end

function RDD:cartesian(other)
  local t = {}
  _.forEach(self:collect(), function(x)
    _.forEach(other:collect(), function(y)
      table.insert(t, {x, y})
    end)
  end)
  return self.ctx:parallelize(t)
end

function RDD:coalesce()
  return self
end

function RDD:collect()
  return _iterToArray(self:toLocalIterator())
end

function RDD:context()
  return self.ctx
end

function RDD:count()
  return _.reduce(self.p, function(r, p) return r + p:_count() end, 0)
end

function RDD:countApprox()
  return self:count()
end

function RDD:countByKey()
  return _.reduce(self:collect(), function(r, e)
    local k = e[1]
    if _.isNil(r[k]) then
      r[k] = 1
    else
      r[k] = r[k] + 1
    end
    return r
  end, {})
end

function RDD:countByValue()
  return _.reduce(self:collect(), function(r, n, key)
    if r[n] == nil then
      r[n] = 1
    else
      r[n] = r[n] + 1
    end
    return r
  end, {})
end

function RDD:distinct(numPartitions)
  return self.ctx:parallelize(_.uniq(self:collect()), numPartitions)
end

function RDD:filter(f)
  return self.ctx:parallelize(_.filter(self:collect(), f))
end

function RDD:first()
  return self.p[1].x[1]
end

function RDD:flatMap(f, preservesPartitioning)
  return self:map(f):_flatten()
end

function RDD:flatMapValues(f)
  return self:mapValues(f):_flattenValues()
end

function RDD:fold(zeroValue, op)
  return _.reduce(self:collect(), op, zeroValue)
end

function RDD:foldByKey(zeroValue, op)
  local keys = _.uniq(_.map(self:collect(), function(e) return e[1] end))
  local t = _.map(keys, function(k)
    c = _.map(self:collect(), function(e)
      if e[1] == k then return e[2] end
    end)
    return {k, _.reduce(c, op, zeroValue)}
  end)
  return self.ctx:parallelize(t)
end

function RDD:foreach(f)
  for z, p in ipairs(self.p) do
    for i, x in ipairs(p.x) do
      p.x[i] = f(p.x[i])
    end
  end
end

function RDD:foreachPartition(f)
  for z, p in ipairs(self.p) do
    f(p.x)
  end
end

function RDD:groupBy(f)
  local x = self:collect()
  local keys = _.uniq(_.map(x, f))
  local t = _.map(keys, function(k)
    v = _.reduce(x, function(r, e)
      if f(e) == k then table.insert(r, e) end
      return r
    end, {})
    return {k, v}
  end)
  return self.ctx:parallelize(t)
end

function RDD:groupByKey()
  local keys = _.keys(self:_dict())
  local t = _.map(keys, function(k)
    v = _.reduce(self:collect(), function(r, e)
      if e[1] == k then table.insert(r, e[2]) end
      return r
    end, {})
    return {k, v}
  end)
  return self.ctx:parallelize(t)
end

function RDD:histogram(buckets)
  if _.isNumber(buckets) then
    local num_buckets = buckets
    local min_v = self:min()
    local max_v = self:max()
    buckets = _.map(_.range(0, num_buckets), function(i)
      return min_v + i*(max_v-min_v)/num_buckets
    end)
    local h = self:_histogram(buckets)
    return buckets, h
  end
  local h = self:_histogram(buckets)
  return h
end

function RDD:_histogram(buckets)
  local num_buckets = _.size(buckets) - 1
  local h = {}; _.fill(h, 0, 1, num_buckets)
  _.forEach(self:collect(), function(x)
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
  return self.ctx:parallelize(_.intersection(self:collect(), other:collect()))
end

function RDD:isCheckpointed()
  return false
end

function RDD:join(other)
  local keys = _.intersection(_.keys(self:_dict()), _.keys(other:_dict()))
  local t = _.reduce(keys, function(r, key)
    _.forEach(self:collect(), function(x)
      if x[1] == key then
        _.forEach(other:collect(), function(y)
          if y[1] == key then
            table.insert(r, {key, {x[2], y[2]}})
          end
        end)
      end
    end)
    return r
  end, {})
  return self.ctx:parallelize(t)
end

function RDD:keyBy(f)
  local t = _.map(self:collect(), function(e) return {f(e), e} end)
  return self.ctx:parallelize(t)
end

function RDD:keys(f)
  local t = _.map(self:collect(), function(e) return e[1] end)
  return self.ctx:parallelize(t)
end

function RDD:leftOuterJoin(other)
  local d1 = self:_dict()
  local d2 = other:_dict()
  local t = _.reduce(self:collect(), function(r, e)
      local right = {}
      _.forEach(other:collect(), function(y)
        if y[1] == e[1] then table.insert(right, y[2]) end
      end)
      if _.size(right) == 0 then
        table.insert(r, {e[1], {e[2], nil}})
      else
        _.forEach(right, function(z)
          table.insert(r, {e[1], {e[2], z}})
        end)
      end
    return r
  end, {})
  return self.ctx:parallelize(t)
end

function RDD:lookup(key)
  return _.reduce(self:collect(), function(r, e)
    if e[1] == key then table.insert(r, e[2]) end
    return r
  end, {})
end

function RDD:map(f)
  local t = {}
  for e in self:toLocalIterator() do table.insert(t, f(e)) end
  return self.ctx:parallelize(t)
end

function RDD:mapValues(f)
  local t = _.map(self:collect(), function(e) return {e[1], f(e[2])} end)
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

function RDD:partitions()
  return self.p
end

function RDD:reduce(f)
  return _.reduce(self:collect(), f)
end

function RDD:reduceByKey(f)
  return self:groupByKey():mapValues(function(e) return _.reduce(e, f) end)
end

function RDD:rightOuterJoin(other)
  local d1 = self:_dict()
  local d2 = other:_dict()
  local t = _.reduce(_.keys(d1), function(r, k)
    if d1[k] then v = d1[k] else v = nil end
    r[k] = {v, d2[k]}
    return r
  end, {})
  return self.ctx:parallelize(t)
end

function RDD:stats()
  local x = self:collect()
  local r = _.reduce(x, function(r, v)
    r.count = r.count + 1
    r.sum = r.sum + v
    return r
  end, {count=0, sum=0})
  r.mean = r.sum / r.count
  
  local sumOfSquares = _.reduce(x, function(r, v) return r + v*v end, 0)
  r.stdev = math.sqrt((r.count * sumOfSquares - r.sum * r.sum) / (r.count * (r.count-1)))
  r.sum = nil
  return r
end

function RDD:take(n)
  return _.slice(self:collect(), n)
end

function RDD:takeSample(n)
  return _.sample(self:collect(), n)
end

function RDD:toLocalIterator()
  local pIndex = 1
  local i = 0
  return function()
    if pIndex > #self.p then return nil end
    local partitionData = self.p[pIndex].x
    if not _.isTable(partitionData) then return nil end
    i = i + 1
    if i > #partitionData then
      pIndex = pIndex + 1
      i = 1
      if pIndex > #self.p then return nil end
      partitionData = self.p[pIndex].x
    end
    
    if pIndex <= #self.p and i <= #partitionData then
      return partitionData[i]
    end
  end
end

function RDD:values(f)
  local t = _.map(self:collect(), function(e) return e[2] end)
  return self.ctx:parallelize(t)
end

function RDD:zip(other)
  local t = _.zip(self:collect(), other:collect())
  return self.ctx:parallelize(t)
end

return RDD
