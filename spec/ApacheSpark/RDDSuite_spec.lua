local _ = require 'lodash'
local moses = require 'moses'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Apache Spark 2.2.0 RDDSuite', function()

  local sc = stuart.NewContext()

  it('basic operations', function()
    local nums = sc:makeRDD({1,2,3,4}, 2)
    assert.equals(2, #nums.partitions)
    assert.same({1,2,3,4}, nums:collect())
    assert.same({1,2,3,4}, moses.array(nums:toLocalIterator()))
    local dups = sc:makeRDD({1,1,2,2,3,3,4,4}, 2)
    assert.equals(4, dups:distinct():count())
    assert.equals(4, dups:distinct():count())
    assert.same(dups:distinct():collect(), dups:distinct():collect())
    assert.same(dups:distinct():collect(), dups:distinct(2):collect())
    assert.equals(10, nums:reduce(function(r, x) return r+x end))
    assert.equals(10, nums:fold(0, function(a,b) return a+b end))
    assert.same({'1','2','3','4'}, nums:map(_.str):collect())
    assert.same({3,4}, nums:filter(function(x) return x > 2 end):collect())
    assert.same({1,1,2,1,2,3,1,2,3,4}, nums:flatMap(function(x) return _.range(x) end):collect())
    assert.same({1,2,3,4,1,2,3,4}, nums:union(nums):collect())
    assert.same({{1,2},{3,4}}, nums:glom():map(_.flatten):collect())
    assert.same({'3','4'}, nums:collect(function(i) if i >= 3 then return _.str(i) end end))
    assert.same({{'1',1}, {'2',2}, {'3',3}, {'4',4}}, nums:keyBy(_.str):collect())
    assert.is_false(nums:isEmpty())
    assert.equals(4, nums:max())
    assert.equals(1, nums:min())
    
    local partitionSums = nums:mapPartitions(function(iter)
      local sum = 0
      for v in iter do sum = sum + v end
      local i = 0 
      return function()
        i = i + 1
        if i == 1 then return sum end
      end
    end)
    assert.same({3,7}, partitionSums:collect())

    local partitionSumsWithSplit = nums:mapPartitionsWithIndex(function(split, iter)
      local sum = 0
      for v in iter do sum = sum + v end
      local i = 0 
      return function()
        i = i + 1
        if i == 1 then return {split, sum} end
      end
    end)
    assert.contains_pair(partitionSumsWithSplit:collect(), {0,3})
    assert.contains_pair(partitionSumsWithSplit:collect(), {1,7})
  end)

  it('SparkContext.union', function()
    local nums = sc:makeRDD({1, 2, 3, 4}, 2)
    assert.same({1, 2, 3, 4}, sc:union({nums}):collect())
    assert.same({1, 2, 3, 4, 1, 2, 3, 4}, sc:union({nums, nums}):collect())
    --Scala-specific: assert.same({1, 2, 3, 4}, sc:union(Seq(nums)):collect())
    --Scala-specific: assert.same({1, 2, 3, 4, 1, 2, 3, 4}, sc:union(Seq(nums, nums)):collect())
  end)

  -- translation of this test to Lua avoids the variable name "pair"
  it('aggregate', function()
    local pairsrdd = sc:makeRDD({{'a',1}, {'b',2}, {'a',2}, {'c',5}, {'a',3}}, 2)
    local mergeElement = function(map, pairrdd)
      map[pairrdd[1]] = (map[pairrdd[1]] or 0) + pairrdd[2]
      return map
    end
    local mergeMaps = function(map1, map2)
      local r = map1
      for key,value in pairs(map2) do
        r[key] = (r[key] or 0) + value 
      end
      return r
    end
    local result = pairsrdd:aggregate({}, mergeElement, mergeMaps)
    assert.equals(6, result['a'])
    assert.equals(2, result['b'])
    assert.equals(5, result['c'])
  end)

  it('repartitioned RDDs', function()
    local data = sc:parallelize(_.range(1, 1000), 10)
    
    -- Coalesce partitions
    local repartitioned1 = data:repartition(2)
    assert.equals(2, #repartitioned1.partitions)
    local partitions1 = repartitioned1:glom():collect()
    assert.is_true(#partitions1[1] > 0)
    assert.is_true(#partitions1[2] > 0)
    assert.same(_.range(1, 1000), repartitioned1:collect())
    
    -- Split partitions
    local repartitioned2 = data:repartition(20)
    assert(20, #repartitioned2.partitions)
    local partitions2 = repartitioned2:glom():collect()
    assert.is_true(#partitions2[1] > 0)
    assert.is_true(#partitions2[20] > 0)
    assert.same(_.range(1, 1000), repartitioned2:collect())
  end)
  
end)
