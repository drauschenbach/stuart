local moses = require 'moses'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Pysparkling test_streaming_queue.py', function()

  local sum = function(x) return moses.reduce(x, function(r,v) return r+v end) end

  it('count()', function()
    local sc = stuart.NewContext()
    local ssc = stuart.NewStreamingContext(sc, 0.1)

    local result = {}
    ssc:queueStream({moses.range(1,20), {'a', 'b'}, {'c'}})
      :count()
      :foreachRDD(function(rdd) result[#result+1] = rdd:collect()[1] end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.3)
    ssc:stop()
    
    assert.equals(23, sum(result))
  end)

  it('groupByKey()', function()
    local sc = stuart.NewContext()
    local ssc = stuart.NewStreamingContext(sc, 0.1)

    local result = {}
    ssc:queueStream({
      sc:makeRDD({{'a',5}, {'b',8}, {'a',2}}),
      sc:makeRDD({{'a',2}, {'b',3}})
     })
      :groupByKey()
      --:mapPartitions(sort)
      --:mapValues(sort)
      :foreachRDD(function(rdd) result[#result+1] = rdd:collect() end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.25)
    ssc:stop()
    
    assert.contains_keyed_pair(result[1], 'a', {5,2}) -- hack: {2,5} was flipped to {5,2} due to missing sort
    assert.contains_keyed_pair(result[1], 'b', {8})
    assert.contains_keyed_pair(result[2], 'a', {2})
    assert.contains_keyed_pair(result[2], 'b', {3})
  end)
  
  it('mapValues()', function()
    local sc = stuart.NewContext()
    local ssc = stuart.NewStreamingContext(sc, 0.1)

    local result = {}
    ssc:queueStream({{{'a', {5,8,2}}, {'b', {6,3,8}}}})
      :mapValues(moses.sort)
      :foreachRDD(function(rdd) result[#result+1] = rdd:collect() end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.15)
    ssc:stop()
    
    assert.contains_keyed_pair(result[1], 'a', {2,5,8})
    assert.contains_keyed_pair(result[1], 'b', {3,6,8})
  end)
  
end)
