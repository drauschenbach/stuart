local moses = require 'moses'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('DStream', function()

  local sc = stuart.NewContext()

  it('can groupByKey()', function()
    local ssc = stuart.NewStreamingContext(sc, .05)
    local dstream = ssc:queueStream({
      sc:makeRDD({{2,3}, {2,4}}),
      sc:makeRDD({{3,7}, {3,8}}),
      sc:makeRDD({{3,9}})
    })
    
    local result = {}
    dstream:groupByKey():foreachRDD(function(rdd) result[#result+1] = rdd:collect() end)
    
    ssc:start()
    ssc:awaitTerminationOrTimeout(.3)
    ssc:stop()
    
    assert.contains_keyed_pair(result[1], 2, {3,4})
    assert.contains_keyed_pair(result[2], 3, {7,8})
    assert.contains_keyed_pair(result[3], 3, {9})
  end)
  
end)
