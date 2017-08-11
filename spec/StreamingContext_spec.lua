local _ = require 'lodash'
local inspect = require 'inspect'
local registerAsserts = require 'registerAsserts'
local socket = require 'socket'
local stuart = require 'stuart'

registerAsserts(assert)

describe('StreamingContext', function()

  local sc = stuart.NewContext()
  
  it('can timeout from awaitTerminationOrTimeout()', function()
    local timeoutSecs = .1
    local ssc = stuart.NewStreamingContext(sc)
    local startTime = socket.gettime() * 1000
    ssc:awaitTerminationOrTimeout(timeoutSecs)
    local endTime = socket.gettime() * 1000
    local elapsedTime = endTime - startTime
    assert.is_in_range(elapsedTime, 100, 5000)
  end)
  
  it('can queue RDDs into a DStream', function()
    local timeoutSecs = .3
    local rdd1 = sc:parallelize({'a', 'b'})
    local rdd2 = sc:parallelize({'c'})
    local rdds = {rdd1, rdd2}
    local ssc = stuart.NewStreamingContext(sc)
    local dstream = ssc:queueStream(rdds)
    
    local r = {}
    dstream:foreachRDD(function(rdd)
      _.forEach(rdd:collect(), function(e) table.insert(r, e) end)
    end)
    
    ssc:awaitTerminationOrTimeout(timeoutSecs)
    assert.contains(r, 'a')
    assert.contains(r, 'b')
    assert.contains(r, 'c')
  end)

end)
