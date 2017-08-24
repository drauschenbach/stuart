local _ = require 'lodash'
local inspect = require 'inspect'
local moses = require 'moses'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Pysparkling test_streaming_queue.py', function()

  function sum(x) return _.reduce(x, function(r,v) return r+v end) end

  it('count()', function()
    local sc = stuart.NewContext()
    local ssc = stuart.NewStreamingContext(sc, 0.1)

    local result = {}
    ssc:queueStream({_.range(20), {'a', 'b'}, {'c'}})
      :count()
      :foreachRDD(function(rdd) result[#result+1] = rdd:collect()[1] end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.3)
    
    assert.equals(23, sum(result))
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
    
    assert.contains_keyed_pair(result[1], 'a', {2,5,8})
    assert.contains_keyed_pair(result[1], 'b', {3,6,8})
  end)
  
end)
