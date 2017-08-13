local _ = require 'lodash'
local moses = require 'moses'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Apache Spark 2.2.0 Unit Tests', function()

  local sc = stuart.NewContext()

  it('basic operations', function()
    local nums = sc:makeRDD({1,2,3,4}, 2)
    assert.equals(2, #nums:partitions())
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

  it("SparkContext.union", function()
    local nums = sc:makeRDD({1, 2, 3, 4}, 2)
    assert.same({1, 2, 3, 4}, sc:union({nums}):collect())
    assert.same({1, 2, 3, 4, 1, 2, 3, 4}, sc:union({nums, nums}):collect())
    --Scala-specific: assert.same({1, 2, 3, 4}, sc:union(Seq(nums)):collect())
    --Scala-specific: assert.same({1, 2, 3, 4, 1, 2, 3, 4}, sc:union(Seq(nums, nums)):collect())
  end)

end)
