local moses = require 'moses'
local now = require 'stuart.interface'.now
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('StreamingContext', function()

  local sc = stuart.NewContext()

  it('can create multiple independent StreamingContext objects', function()
    local ssc1 = stuart.NewStreamingContext(sc)
    assert.equals(0, #ssc1.dstreams)
    ssc1:queueStream({sc:makeRDD(moses.range(1,3))})
    assert.equals(1, #ssc1.dstreams)

    local ssc2 = stuart.NewStreamingContext(sc)
    assert.equals(0, #ssc2.dstreams)
    ssc2:queueStream({sc:makeRDD(moses.range(4,6))})
    assert.equals(1, #ssc2.dstreams)
  end)
  
  it('can timeout from awaitTerminationOrTimeout()', function()
    local timeoutSecs = .1
    local ssc = stuart.NewStreamingContext(sc)
    local startTime = now() * 1000
    ssc:start()
    ssc:awaitTerminationOrTimeout(timeoutSecs)
    local endTime = now() * 1000
    local elapsedTime = endTime - startTime
    assert.is_in_range(elapsedTime, 100, 5000)
  end)
  
  it('can queue RDDs into a DStream', function()
    local ssc = stuart.NewStreamingContext(sc, 0.1)
    local rdd1 = sc:parallelize({'a', 'b'})
    local rdd2 = sc:parallelize({'c'})
    local rdds = {rdd1, rdd2}
    local dstream = ssc:queueStream(rdds)
    
    local r = {}
    dstream:foreachRDD(function(rdd)
      moses.forEach(rdd:collect(), function(e) table.insert(r, e) end)
    end)
    
    ssc:start()
    ssc:awaitTerminationOrTimeout(0.3)
    assert.contains(r, 'a')
    assert.contains(r, 'b')
    assert.contains(r, 'c')
  end)
  
  it('count()', function()
    local ssc = stuart.NewStreamingContext(sc, 0.1)
    local result = {}
    local dstream = ssc:queueStream({sc:makeRDD(moses.range(1,10)), sc:makeRDD({20,21})})
    dstream = dstream:count()
    dstream:foreachRDD(function(rdd)
      moses.forEach(rdd:collect(), function(e) result[#result+1] = e end)
    end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.25)
    
    local count = moses.reduce(result, function(r,e) return r+e end, 0)
    assert.equals(12, count)
  end)
  
  it('transform 1', function()
    local ssc = stuart.NewStreamingContext(sc, 0.1)
    local result = {}
    local dstream = ssc:queueStream({sc:makeRDD({1,2,3})})
    local x = dstream:transform(function(rdd)
      assert.same({1,2,3}, rdd:collect())
      return rdd:map(function(x) return x+1 end)
    end)
    x:foreachRDD(function(rdd)
      assert.same({2,3,4}, rdd:collect())
      result[#result+1] = {rdd:min(), rdd:max()}
    end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.15)
    
    assert.contains_pair(result, {2,4})
  end)
  
  it('transform 2', function()
    local ssc = stuart.NewStreamingContext(sc, 0.1)
    local result = {}
    local dstream = ssc:queueStream({sc:makeRDD({1,2,3}), sc:makeRDD({20,21})})
    local x = dstream:transform(function(rdd)
      return rdd:map(function(x) return x+1 end)
    end)
    x:foreachRDD(function(rdd) result[#result+1] = {rdd:min(), rdd:max()} end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.25)
    
    assert.contains_pair(result, {2,4})
    assert.contains_pair(result, {21,22})
  end)
  
  it('transform 3', function()
    local ssc = stuart.NewStreamingContext(sc, 0.1)
    local result = {}
    ssc:queueStream({sc:makeRDD({1,2,3}), sc:makeRDD({5,6,7})})
      :transform(function(rdd)
        return rdd:map(function(x) return x+1 end) end)
      :transform(function(rdd)
        return rdd:map(function(x) return x*2 end) end)
      :foreachRDD(function(rdd)
        result[#result+1] = rdd:collect() end)

    ssc:start()
    ssc:awaitTerminationOrTimeout(0.25)
    
    assert.contains_pair(result, {4,6,8})
    assert.contains_pair(result, {12,14,16})
  end)
  
end)
