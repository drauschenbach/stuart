local Context = require 'stuart.Context'
local moses = require 'moses'
moses.range = require 'stuart.util.mosesPatchedRange'
local registerAsserts = require 'registerAsserts'
local StreamingContext = require 'stuart.streaming.StreamingContext'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Apache Spark 2.2.0 StreamingContextSuite', function()

  local master = 'local[1]'
  local appName = debug.getinfo(1,'S').short_src
  local batchDuration = 0.5 -- 500 milliseconds
  local sparkHome = 'someDir'

  it('from no conf constructor', function()
    local ssc = stuart.NewStreamingContext(master, appName, batchDuration)
    assert.equal(master, ssc.sparkContext.conf:get('spark.master'))
    assert.equal(appName, ssc.sparkContext.conf:get('spark.app.name'))
  end)
  
  it('from no conf + spark home', function()
    local ssc = stuart.NewStreamingContext(master, appName, batchDuration, sparkHome, nil)
    assert.equal(sparkHome, ssc.conf:get('spark.home'))
  end)

  it('from existing SparkContext', function()
    local sc = Context:new(master, appName)
    local ssc = StreamingContext:new(sc, batchDuration)
    assert.not_nil(ssc)
  end)

  it('start and stop state check', function()
    local ssc = stuart.NewStreamingContext(master, appName, batchDuration)
    assert.equals('initialized', ssc:getState())
    ssc:start()
    assert.equals('active', ssc:getState())
    ssc:stop()
    assert.equals('stopped', ssc:getState())
  end)

  it('start multiple times', function()
    local ssc = stuart.NewStreamingContext(master, appName, batchDuration)
    ssc:start()
    assert.equals('active', ssc:getState())
    ssc:start()
    assert.equals('active', ssc:getState())
  end)

  it('stop multiple times', function()
    local ssc = stuart.NewStreamingContext(master, appName, batchDuration)
    ssc:start()
    ssc:stop()
    assert.equals('stopped', ssc:getState())
    ssc:stop()
    assert.equals('stopped', ssc:getState())
  end)

  it('stop before start', function()
    local ssc = stuart.NewStreamingContext(master, appName, batchDuration)
    ssc:stop() -- stop before start should not raise an error
    assert.equals('stopped', ssc:getState())
  end)

  it('start after stop', function()
    local ssc = stuart.NewStreamingContext(master, appName, batchDuration)
    ssc:stop()
    assert.has_error(function()
      ssc:start() -- start after stop should raise an error
    end)
    assert.equals('stopped', ssc:getState())
  end)
  
end)
