local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Pysparkling DStream examples', function()

  it('countByWindow() Applies count() after window().', function()
    local sc = stuart.NewContext()
    local ssc = stuart.NewStreamingContext(sc, .1)
    local results = {}
    ssc:queueStream({sc:makeRDD({1,1,5}), sc:makeRDD({5,5,2,4}), sc:makeRDD({1,2})})
      :countByWindow(0.2)
      :foreachRDD(function(rdd)
        results[#results+1] = rdd:collect()
      end)
    
    ssc:start()
    ssc:awaitTerminationOrTimeout(.35)
    ssc:stop()
    
    assert.same({3}, results[1])
    assert.same({7}, results[2])
    assert.same({6}, results[3])
  end)
  
  it('map() Apply function f', function()
    local sc = stuart.NewContext()
    local ssc = stuart.NewStreamingContext(sc, .1)
    local results = {}
    ssc:queueStream({sc:makeRDD({4}), sc:makeRDD({2}), sc:makeRDD({7})})
      :map(function(e) return e+1 end)
      :foreachRDD(function(rdd)
        results[#results+1] = rdd:collect()
      end)
    
    ssc:start()
    ssc:awaitTerminationOrTimeout(.35)
    ssc:stop()
    
    assert.same({5}, results[1])
    assert.same({3}, results[2])
    assert.same({8}, results[3])
  end)
  
  it('window() Windowed RDD.', function()
    local sc = stuart.NewContext()
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
