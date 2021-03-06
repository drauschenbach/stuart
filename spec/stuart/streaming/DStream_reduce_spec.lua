local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('DStream', function()

  it('can reduce()', function()
    --import org.apache.spark.streaming.{Seconds, Milliseconds, StreamingContext}
    --import scala.collection.mutable.SynchronizedQueue
    --import org.apache.spark.rdd.RDD
    --
    --val ssc = new StreamingContext(sc, Milliseconds(100))
    --val rddQueue = new SynchronizedQueue[RDD[Int]]()
    --val inputStream = ssc.queueStream(rddQueue)
    --
    --rddQueue += sc.makeRDD(List(1,1,5))
    --rddQueue += sc.makeRDD(List(5,5,2,4))
    --rddQueue += sc.makeRDD(List(1,2))
    --
    --val result = inputStream.reduce((x,y) => x+y)
    --result.foreachRDD(rdd =>
    --  rdd.toDF().show()
    --)
    --ssc.start()
    --ssc.awaitTerminationOrTimeout(450)
    --ssc.stop()
    
    local sc = stuart.NewContext()
    local ssc = stuart.NewStreamingContext(sc, .1)
    local results = {}
    ssc:queueStream({sc:makeRDD({1,1,5}), sc:makeRDD({5,5,2,4}), sc:makeRDD({1,2})})
      :reduce(function(r, e)
        return r+e
      end)
      :foreachRDD(function(rdd)
        results[#results+1] = rdd:collect()
      end)
    
    ssc:start()
    ssc:awaitTerminationOrTimeout(.35)
    ssc:stop()
    
    assert.same({7}, results[1])
    assert.same({16}, results[2])
    assert.same({3}, results[3])
  end)
  
end)
