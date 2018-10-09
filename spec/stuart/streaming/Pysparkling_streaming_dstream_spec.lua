local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Pysparkling DStream examples', function()

  local sc = stuart.NewContext()

  it('window() Windowed RDD.', function()
    local ssc = stuart.NewStreamingContext(sc, .2)
    local results = {}
    ssc:queueStream({sc:makeRDD({1}), sc:makeRDD({2}), sc:makeRDD({3}), sc:makeRDD({4}), sc:makeRDD({5}), sc:makeRDD({6})})
      :window(0.6)
      :foreachRDD(function(rdd)
        results[#results+1] = rdd:collect()
      end)
    
    ssc:start()
    ssc:awaitTerminationOrTimeout(1.3)
    ssc:stop()
    
    assert.same({1}, results[1])
    assert.same({1,2}, results[2])
    assert.same({1,2,3}, results[3])
    assert.same({2,3,4}, results[4])
    assert.same({3,4,5}, results[5])
    assert.same({4,5,6}, results[6])
  end)
  
end)
