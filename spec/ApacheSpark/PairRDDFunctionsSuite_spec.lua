local lodashFind = require 'stuart.lodashFind'
local moses = require 'moses'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('Apache Spark 2.2.0 PairRDDFunctionsSuite', function()

  local sc = stuart.NewContext()
  
--  it("aggregateByKey", function()
--    val pairs = sc.parallelize(Array((1, 1), (1, 1), (3, 2), (5, 1), (5, 3)), 2)
--
--    val sets = pairs.aggregateByKey(new HashSet[Int]())(_ += _, _ ++= _).collect()
--    assert(sets.size === 3)
--    val valuesFor1 = sets.find(_._1 == 1).get._2
--    assert(valuesFor1.toList.sorted === List(1))
--    val valuesFor3 = sets.find(_._1 == 3).get._2
--    assert(valuesFor3.toList.sorted === List(2))
--    val valuesFor5 = sets.find(_._1 == 5).get._2
--    assert(valuesFor5.toList.sorted === List(1, 3))
--  end)

  it("groupByKey", function()
    local pairs = sc:parallelize({{1,1}, {1,2}, {1,3}, {2,1}})
    local groups = pairs:groupByKey():collect()
    assert.equals(2, #groups)
    local valuesFor1 = lodashFind(groups, function(v) return v[1] == 1 end)[2]
    assert.same({1,2,3}, valuesFor1)
    local valuesFor2 = lodashFind(groups, function(v) return v[1] == 2 end)[2]
    assert.same({1}, valuesFor2)
  end)

  it("groupByKey with duplicates", function()
    local pairs = sc:parallelize({{1,1}, {1,2}, {1,3}, {1,1}, {2,1}})
    local groups = pairs:groupByKey():collect()
    assert.equals(2, #groups)
    local valuesFor1 = lodashFind(groups, function(v) return v[1] == 1 end)[2]
    table.sort(valuesFor1)
    assert.same({1,1,2,3}, valuesFor1)
    local valuesFor2 = lodashFind(groups, function(v) return v[1] == 2 end)[2]
    assert.same({1}, valuesFor2)
  end)

  it("groupByKey with negative key hash codes", function()
    local pairs = sc:parallelize({{-1,1}, {-1,2}, {-1,3}, {2,1}})
    local groups = pairs:groupByKey():collect()
    assert.equals(2, #groups)
    local valuesForMinus1 = lodashFind(groups, function(v) return v[1] == -1 end)[2]
    table.sort(valuesForMinus1)
    assert.same({1,2,3}, valuesForMinus1)
    local valuesFor2 = lodashFind(groups, function(v) return v[1] == 2 end)[2]
    assert.same({1}, valuesFor2)
  end)

  it("groupByKey with many output partitions", function()
    local pairs = sc:parallelize({{1,1}, {1,2}, {1,3}, {2,1}})
    local groups = pairs:groupByKey(10):collect()
    assert.equals(2, #groups)
    local valuesFor1 = lodashFind(groups, function(v) return v[1] == 1 end)[2]
    table.sort(valuesFor1)
    assert.same({1,2,3}, valuesFor1)
    local valuesFor2 = lodashFind(groups, function(v) return v[1] == 2 end)[2]
    assert.same({1}, valuesFor2)
  end)

--  it("sampleByKey", function()
--
--    val defaultSeed = 1L
--
--    // vary RDD size
--    for (n <- List(100, 1000, 1000000)) {
--      val data = sc.parallelize(1 to n, 2)
--      val fractionPositive = 0.3
--      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
--      val samplingRate = 0.1
--      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, defaultSeed, n)
--    }
--
--    // vary fractionPositive
--    for (fractionPositive <- List(0.1, 0.3, 0.5, 0.7, 0.9)) {
--      val n = 100
--      val data = sc.parallelize(1 to n, 2)
--      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
--      val samplingRate = 0.1
--      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, defaultSeed, n)
--    }
--
--    // Use the same data for the rest of the tests
--    val fractionPositive = 0.3
--    val n = 100
--    val data = sc.parallelize(1 to n, 2)
--    val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
--
--    // vary seed
--    for (seed <- defaultSeed to defaultSeed + 5L) {
--      val samplingRate = 0.1
--      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, seed, n)
--    }
--
--    // vary sampling rate
--    for (samplingRate <- List(0.01, 0.05, 0.1, 0.5)) {
--      StratifiedAuxiliary.testSample(stratifiedData, samplingRate, defaultSeed, n)
--    }
--  end)

--  it("sampleByKeyExact", function()
--    val defaultSeed = 1L
--
--    // vary RDD size
--    for (n <- List(100, 1000, 1000000)) {
--      val data = sc.parallelize(1 to n, 2)
--      val fractionPositive = 0.3
--      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
--      val samplingRate = 0.1
--      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, defaultSeed, n)
--    }
--
--    // vary fractionPositive
--    for (fractionPositive <- List(0.1, 0.3, 0.5, 0.7, 0.9)) {
--      val n = 100
--      val data = sc.parallelize(1 to n, 2)
--      val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
--      val samplingRate = 0.1
--      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, defaultSeed, n)
--    }
--
--    // Use the same data for the rest of the tests
--    val fractionPositive = 0.3
--    val n = 100
--    val data = sc.parallelize(1 to n, 2)
--    val stratifiedData = data.keyBy(StratifiedAuxiliary.stratifier(fractionPositive))
--
--    // vary seed
--    for (seed <- defaultSeed to defaultSeed + 5L) {
--      val samplingRate = 0.1
--      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, seed, n)
--    }
--
--    // vary sampling rate
--    for (samplingRate <- List(0.01, 0.05, 0.1, 0.5)) {
--      StratifiedAuxiliary.testSampleExact(stratifiedData, samplingRate, defaultSeed, n)
--    }
--  end)

  it("reduceByKey", function()
    local pairs = sc:parallelize({{1,1}, {1,2}, {1,3}, {1,1}, {2,1}})
    local sums = pairs:reduceByKey(function(r,x) return r+x end):collect()
    assert.contains_pair(sums, {1,7})
    assert.contains_pair(sums, {2,1})
  end)

  it("reduceByKey with collectAsMap", function()
    local pairs = sc:parallelize({{1,1}, {1,2}, {1,3}, {1,1}, {2,1}})
    local sums = pairs:reduceByKey(function(x,y) return x+y end):collectAsMap()
    assert.equals(2, #sums)
    assert.equals(7, sums[1])
    assert.equals(1, sums[2])
  end)

  it("reduceByKey with many output partitions", function()
    local pairs = sc:parallelize({{1,1}, {1,2}, {1,3}, {1,1}, {2,1}})
    local sums = pairs:reduceByKey(function(x,y) return x+y end, 10):collect()
    assert.contains_pair(sums, {1,7})
    assert.contains_pair(sums, {2,1})
  end)

--  it("reduceByKey with partitioner", function()
--    val p = new Partitioner() {
--      def numPartitions = 2
--      def getPartition(key: Any) = key.asInstanceOf[Int]
--    }
--    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 1), (0, 1))).partitionBy(p)
--    val sums = pairs.reduceByKey(_ + _)
--    assert(sums.collect().toSet === Set((1, 4), (0, 1)))
--    assert(sums.partitioner === Some(p))
--    // count the dependencies to make sure there is only 1 ShuffledRDD
--    val deps = new HashSet[RDD[_]]()
--    def visit(r: RDD[_]) {
--      for (dep <- r.dependencies) {
--        deps += dep.rdd
--        visit(dep.rdd)
--      }
--    }
--    visit(sums)
--    assert(deps.size === 2) // ShuffledRDD, ParallelCollection.
--  end)

--  it("countApproxDistinctByKey", function()
--    def error(est: Long, size: Long): Double = math.abs(est - size) / size.toDouble
--
--    /* Since HyperLogLog unique counting is approximate, and the relative standard deviation is
--     * only a statistical bound, the tests can fail for large values of relativeSD. We will be using
--     * relatively tight error bounds to check correctness of functionality rather than checking
--     * whether the approximation conforms with the requested bound.
--     */
--    val p = 20
--    val sp = 0
--    // When p = 20, the relative accuracy is about 0.001. So with high probability, the
--    // relative error should be smaller than the threshold 0.01 we use here.
--    val relativeSD = 0.01
--
--    // For each value i, there are i tuples with first element equal to i.
--    // Therefore, the expected count for key i would be i.
--    val stacked = (1 to 100).flatMap(i => (1 to i).map(j => (i, j)))
--    val rdd1 = sc.parallelize(stacked)
--    val counted1 = rdd1.countApproxDistinctByKey(p, sp).collect()
--    counted1.foreach { case (k, count) => assert(error(count, k) < relativeSD) }
--
--    val rnd = new Random(42)
--
--    // The expected count for key num would be num
--    val randStacked = (1 to 100).flatMap { i =>
--      val num = rnd.nextInt() % 500
--      (1 to num).map(j => (num, j))
--    }
--    val rdd2 = sc.parallelize(randStacked)
--    val counted2 = rdd2.countApproxDistinctByKey(relativeSD).collect()
--    counted2.foreach { case (k, count) =>
--      assert(error(count, k) < relativeSD, s"${error(count, k)} < $relativeSD")
--    }
--  end)

  it("join", function()
    local rdd1 = sc:parallelize({{1,1}, {1,2}, {2,1}, {3,1}})
    local rdd2 = sc:parallelize({{1,'x'}, {2,'y'}, {2,'z'}, {4,'w'}})
    local joined = rdd1:join(rdd2):collect()
    assert.equals(4, #joined)
    assert.contains_keyed_pair(joined, 1, {1,'x'})
    assert.contains_keyed_pair(joined, 1, {2,'x'})
    assert.contains_keyed_pair(joined, 2, {1,'y'})
    assert.contains_keyed_pair(joined, 2, {1,'z'})
  end)

  it("join all-to-all", function()
    local rdd1 = sc:parallelize({{1,1}, {1,2}, {1,3}})
    local rdd2 = sc:parallelize({{1,'x'}, {1,'y'}})
    local joined = rdd1:join(rdd2):collect()
    assert.equals(6, #joined)
    assert.contains_keyed_pair(joined, 1, {1,'x'})
    assert.contains_keyed_pair(joined, 1, {1,'y'})
    assert.contains_keyed_pair(joined, 1, {2,'x'})
    assert.contains_keyed_pair(joined, 1, {2,'y'})
    assert.contains_keyed_pair(joined, 1, {3,'x'})
    assert.contains_keyed_pair(joined, 1, {3,'y'})
  end)

  it("leftOuterJoin", function()
    local rdd1 = sc:parallelize({{1,1}, {1,2}, {2,1}, {3,1}})
    local rdd2 = sc:parallelize({{1,'x'}, {2,'y'}, {2,'z'}, {4,'w'}})
    local joined = rdd1:leftOuterJoin(rdd2):collect()
    assert.equals(5, #joined)
    assert.contains_keyed_pair(joined, 1, {1,'x'})
    assert.contains_keyed_pair(joined, 1, {2,'x'})
    assert.contains_keyed_pair(joined, 2, {1,'y'})
    assert.contains_keyed_pair(joined, 2, {1,'z'})
    assert.contains_keyed_pair(joined, 3, {1,nil})
  end)

  -- See SPARK-9326
--  it("cogroup with empty RDD", function()
--    import scala.reflect.classTag
--    val intPairCT = classTag[(Int, Int)]
--
--    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
--    val rdd2 = sc.emptyRDD[(Int, Int)](intPairCT)
--
--    val joined = rdd1.cogroup(rdd2).collect()
--    assert(joined.size > 0)
--  end)

  -- See SPARK-9326
--  it("cogroup with groupByed RDD having 0 partitions", function()
--    import scala.reflect.classTag
--    val intCT = classTag[Int]
--
--    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
--    val rdd2 = sc.emptyRDD[Int](intCT).groupBy((x) => 5)
--    val joined = rdd1.cogroup(rdd2).collect()
--    assert(joined.size > 0)
--  end)

  it("rightOuterJoin", function()
    local rdd1 = sc:parallelize({{1,1}, {1,2}, {2,1}, {3,1}})
    local rdd2 = sc:parallelize({{1,'x'}, {2,'y'}, {2,'z'}, {4,'w'}})
    local joined = rdd1:rightOuterJoin(rdd2):collect()
    assert.equals(5, #joined)
    assert.contains_keyed_pair(joined, 1, {1,'x'})
    assert.contains_keyed_pair(joined, 1, {2,'x'})
    assert.contains_keyed_pair(joined, 2, {1,'y'})
    assert.contains_keyed_pair(joined, 2, {1,'z'})
    assert.contains_keyed_pair(joined, 4, {nil,'w'})
  end)

--  it("fullOuterJoin", function()
--    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
--    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
--    val joined = rdd1.fullOuterJoin(rdd2).collect()
--    assert(joined.size === 6)
--    assert(joined.toSet === Set(
--      (1, (Some(1), Some('x'))),
--      (1, (Some(2), Some('x'))),
--      (2, (Some(1), Some('y'))),
--      (2, (Some(1), Some('z'))),
--      (3, (Some(1), None)),
--      (4, (None, Some('w')))
--    ))
--  end)

  it("join with no matches", function()
    local rdd1 = sc:parallelize({{1,1}, {1,2}, {2,1}, {3,1}})
    local rdd2 = sc:parallelize({{4,'x'}, {5,'y'}, {5,'z'}, {6,'w'}})
    local joined = rdd1:join(rdd2):collect()
    assert.equals(0, #joined)
  end)

  it("join with many output partitions", function()
    local rdd1 = sc:parallelize({{1,1}, {1,2}, {2,1}, {3,1}})
    local rdd2 = sc:parallelize({{1,'x'}, {2,'y'}, {2,'z'}, {4,'w'}})
    local joined = rdd1:join(rdd2, 10):collect()
    assert.equals(4, #joined)
    assert.contains_keyed_pair(joined, 1, {1,'x'})
    assert.contains_keyed_pair(joined, 1, {2,'x'})
    assert.contains_keyed_pair(joined, 2, {1,'y'})
    assert.contains_keyed_pair(joined, 2, {1,'z'})
  end)

--  it("groupWith", function()
--    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
--    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
--    val joined = rdd1.groupWith(rdd2).collect()
--    assert(joined.size === 4)
--    val joinedSet = joined.map(x => (x._1, (x._2._1.toList, x._2._2.toList))).toSet
--    assert(joinedSet === Set(
--      (1, (List(1, 2), List('x'))),
--      (2, (List(1), List('y', 'z'))),
--      (3, (List(1), List())),
--      (4, (List(), List('w')))
--    ))
--  end)

--  it("groupWith3", function()
--    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
--    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
--    val rdd3 = sc.parallelize(Array((1, 'a'), (3, 'b'), (4, 'c'), (4, 'd')))
--    val joined = rdd1.groupWith(rdd2, rdd3).collect()
--    assert(joined.size === 4)
--    val joinedSet = joined.map(x => (x._1,
--      (x._2._1.toList, x._2._2.toList, x._2._3.toList))).toSet
--    assert(joinedSet === Set(
--      (1, (List(1, 2), List('x'), List('a'))),
--      (2, (List(1), List('y', 'z'), List())),
--      (3, (List(1), List(), List('b'))),
--      (4, (List(), List('w'), List('c', 'd')))
--    ))
--  end)

--  it("groupWith4", function()
--    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
--    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
--    val rdd3 = sc.parallelize(Array((1, 'a'), (3, 'b'), (4, 'c'), (4, 'd')))
--    val rdd4 = sc.parallelize(Array((2, '@')))
--    val joined = rdd1.groupWith(rdd2, rdd3, rdd4).collect()
--    assert(joined.size === 4)
--    val joinedSet = joined.map(x => (x._1,
--      (x._2._1.toList, x._2._2.toList, x._2._3.toList, x._2._4.toList))).toSet
--    assert(joinedSet === Set(
--      (1, (List(1, 2), List('x'), List('a'), List())),
--      (2, (List(1), List('y', 'z'), List(), List('@'))),
--      (3, (List(1), List(), List('b'), List())),
--      (4, (List(), List('w'), List('c', 'd'), List()))
--    ))
--  end)

--  it("zero-partition RDD", function()
--    val emptyDir = Utils.createTempDir()
--    try {
--      val file = sc.textFile(emptyDir.getAbsolutePath)
--      assert(file.partitions.isEmpty)
--      assert(file.collect().toList === Nil)
--      // Test that a shuffle on the file works, because this used to be a bug
--      assert(file.map(line => (line, 1)).reduceByKey(_ + _).collect().toList === Nil)
--    } finally {
--      Utils.deleteRecursively(emptyDir)
--    }
--  end)

  it("keys and values", function()
    local rdd = sc:parallelize({{1,'a'}, {2,'b'}})
    assert.same({1,2}, rdd:keys():collect())
    assert.same({'a','b'}, rdd:values():collect())
  end)

  it("default partitioner uses partition size", function()
    -- specify 2000 partitions
    local a = sc:makeRDD({1,2,3,4}, 2000)
    -- do a map, which loses the partitioner
    local b = a:map(function(a) return {a, tostring(a*2)} end)
    -- then a group by, and see we didn't revert to 2 partitions
    local c = b:groupByKey()
    assert.equals(2000, #c.partitions)
  end)

  it("default partitioner uses largest partitioner", function()
    local a = sc:makeRDD({{1,'a'}, {2,'b'}}, 2)
    local b = sc:makeRDD({{1,'a'}, {2,'b'}}, 2000)
    local c = a:join(b)
    assert.equals(2000, #c.partitions)
  end)

  it("subtract", function()
    local a = sc:parallelize({1,2,3}, 2)
    local b = sc:parallelize({2,3,4}, 4)
    local c = a:subtract(b)
    assert.same({1}, c:collect())
    assert.equals(#a.partitions, #c.partitions)
  end)

--  it("subtract with narrow dependency", function()
--    // use a deterministic partitioner
--    val p = new Partitioner() {
--      def numPartitions = 5
--      def getPartition(key: Any) = key.asInstanceOf[Int]
--    }
--    // partitionBy so we have a narrow dependency
--    val a = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"))).partitionBy(p)
--    // more partitions/no partitioner so a shuffle dependency
--    val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4)
--    val c = a.subtract(b)
--    assert(c.collect().toSet === Set((1, "a"), (3, "c")))
--    // Ideally we could keep the original partitioner...
--    assert(c.partitioner === None)
--  end)

  it("subtractByKey", function()
    local a = sc:parallelize({{1,"a"}, {1,"a"}, {2,"b"}, {3,"c"}}, 2)
    local b = sc:parallelize({{2,20}, {3,30}, {4,40}}, 4)
    local c = a:subtractByKey(b)
    assert.same({{1,'a'}, {1,'a'}}, c:collect())
    assert.equals(#a.partitions, #c.partitions)
  end)

--  it("subtractByKey with narrow dependency", function()
--    // use a deterministic partitioner
--    val p = new Partitioner() {
--      def numPartitions = 5
--      def getPartition(key: Any) = key.asInstanceOf[Int]
--    }
--    // partitionBy so we have a narrow dependency
--    val a = sc.parallelize(Array((1, "a"), (1, "a"), (2, "b"), (3, "c"))).partitionBy(p)
--    // more partitions/no partitioner so a shuffle dependency
--    val b = sc.parallelize(Array((2, "b"), (3, "cc"), (4, "d")), 4)
--    val c = a.subtractByKey(b)
--    assert(c.collect().toSet === Set((1, "a"), (1, "a")))
--    assert(c.partitioner.get === p)
--  end)

  it("foldByKey", function()
    local pairs = sc:parallelize({{1,1}, {1,2}, {1,3}, {1,1}, {2,1}})
    local sums = pairs:foldByKey(0, function(x,y) return x+y end):collect()
    assert.contains_pair(sums, {1,7})
    assert.contains_pair(sums, {2,1})
  end)

--  it("foldByKey with mutable result type", function()
--    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
--    val bufs = pairs.mapValues(v => ArrayBuffer(v)).cache()
--    // Fold the values using in-place mutation
--    val sums = bufs.foldByKey(new ArrayBuffer[Int])(_ ++= _).collect()
--    assert(sums.toSet === Set((1, ArrayBuffer(1, 2, 3, 1)), (2, ArrayBuffer(1))))
--    // Check that the mutable objects in the original RDD were not changed
--    assert(bufs.collect().toSet === Set(
--      (1, ArrayBuffer(1)),
--      (1, ArrayBuffer(2)),
--      (1, ArrayBuffer(3)),
--      (1, ArrayBuffer(1)),
--      (2, ArrayBuffer(1))))
--  end)

--  it("saveNewAPIHadoopFile should call setConf if format is configurable", function()
--    val pairs = sc.parallelize(Array((new Integer(1), new Integer(1))))
--
--    // No error, non-configurable formats still work
--    pairs.saveAsNewAPIHadoopFile[NewFakeFormat]("ignored")
--
--    /*
--     * Check that configurable formats get configured:
--     * ConfigTestFormat throws an exception if we try to write
--     * to it when setConf hasn't been called first.
--     * Assertion is in ConfigTestFormat.getRecordWriter.
--     */
--    pairs.saveAsNewAPIHadoopFile[ConfigTestFormat]("ignored")
--  end)

--  it("saveAsHadoopFile should respect configured output committers", function()
--    val pairs = sc.parallelize(Array((new Integer(1), new Integer(1))))
--    val conf = new JobConf()
--    conf.setOutputCommitter(classOf[FakeOutputCommitter])
--
--    FakeOutputCommitter.ran = false
--    pairs.saveAsHadoopFile(
--      "ignored", pairs.keyClass, pairs.valueClass, classOf[FakeOutputFormat], conf)
--
--    assert(FakeOutputCommitter.ran, "OutputCommitter was never called")
--  end)

--  it("failure callbacks should be called before calling writer.close() in saveNewAPIHadoopFile", function()
--    val pairs = sc.parallelize(Array((new Integer(1), new Integer(2))), 1)
--
--    FakeWriterWithCallback.calledBy = ""
--    FakeWriterWithCallback.exception = null
--    val e = intercept[SparkException] {
--      pairs.saveAsNewAPIHadoopFile[NewFakeFormatWithCallback]("ignored")
--    }
--    assert(e.getCause.getMessage contains "failed to write")
--
--    assert(FakeWriterWithCallback.calledBy === "write,callback,close")
--    assert(FakeWriterWithCallback.exception != null, "exception should be captured")
--    assert(FakeWriterWithCallback.exception.getMessage contains "failed to write")
--  end)

--  it("failure callbacks should be called before calling writer.close() in saveAsHadoopFile", function()
--    val pairs = sc.parallelize(Array((new Integer(1), new Integer(2))), 1)
--    val conf = new JobConf()
--
--    FakeWriterWithCallback.calledBy = ""
--    FakeWriterWithCallback.exception = null
--    val e = intercept[SparkException] {
--      pairs.saveAsHadoopFile(
--        "ignored", pairs.keyClass, pairs.valueClass, classOf[FakeFormatWithCallback], conf)
--    }
--    assert(e.getMessage contains "failed to write")
--
--    assert(FakeWriterWithCallback.calledBy === "write,callback,close")
--    assert(FakeWriterWithCallback.exception != null, "exception should be captured")
--    assert(FakeWriterWithCallback.exception.getMessage contains "failed to write")
--  end)

  it("lookup", function()
    local pairs = sc:parallelize({{1,2}, {3,4}, {5,6}, {5,7}})

    assert.equals(nil, pairs.partitioner)
    assert.same({2}, pairs:lookup(1))
    assert.same({6,7}, pairs:lookup(5))
    assert.same({}, pairs:lookup(-1))
  end)

--  it("lookup with partitioner", function()
--    val pairs = sc.parallelize(Array((1, 2), (3, 4), (5, 6), (5, 7)))
--
--    val p = new Partitioner {
--      def numPartitions: Int = 2
--
--      def getPartition(key: Any): Int = Math.abs(key.hashCode() % 2)
--    }
--    val shuffled = pairs.partitionBy(p)
--
--    assert(shuffled.partitioner === Some(p))
--    assert(shuffled.lookup(1) === Seq(2))
--    assert(shuffled.lookup(5) === Seq(6, 7))
--    assert(shuffled.lookup(-1) === Seq())
--  end)

--  it("lookup with bad partitioner", function()
--    val pairs = sc.parallelize(Array((1, 2), (3, 4), (5, 6), (5, 7)))
--
--    val p = new Partitioner {
--      def numPartitions: Int = 2
--
--      def getPartition(key: Any): Int = key.hashCode() % 2
--    }
--    val shuffled = pairs.partitionBy(p)
--
--    assert(shuffled.partitioner === Some(p))
--    assert(shuffled.lookup(1) === Seq(2))
--    intercept[IllegalArgumentException] {shuffled.lookup(-1)}
--  end)

--  private object StratifiedAuxiliary {
--    def stratifier (fractionPositive: Double): (Int) => String = {
--      (x: Int) => if (x % 10 < (10 * fractionPositive).toInt) "1" else "0"
--    }
--
--    def assertBinomialSample(
--        exact: Boolean,
--        actual: Int,
--        trials: Int,
--        p: Double): Unit = {
--      if (exact) {
--        assert(actual == math.ceil(p * trials).toInt)
--      } else {
--        val dist = new BinomialDistribution(trials, p)
--        val q = dist.cumulativeProbability(actual)
--        withClue(s"p = $p: trials = $trials") {
--          assert(q >= 0.001 && q <= 0.999)
--        }
--      }
--    }
--
--    def assertPoissonSample(
--        exact: Boolean,
--        actual: Int,
--        trials: Int,
--        p: Double): Unit = {
--      if (exact) {
--        assert(actual == math.ceil(p * trials).toInt)
--      } else {
--        val dist = new PoissonDistribution(p * trials)
--        val q = dist.cumulativeProbability(actual)
--        withClue(s"p = $p: trials = $trials") {
--          assert(q >= 0.001 && q <= 0.999)
--        }
--      }
--    }
--
--    def testSampleExact(stratifiedData: RDD[(String, Int)],
--        samplingRate: Double,
--        seed: Long,
--        n: Long): Unit = {
--      testBernoulli(stratifiedData, true, samplingRate, seed, n)
--      testPoisson(stratifiedData, true, samplingRate, seed, n)
--    }
--
--    def testSample(stratifiedData: RDD[(String, Int)],
--        samplingRate: Double,
--        seed: Long,
--        n: Long): Unit = {
--      testBernoulli(stratifiedData, false, samplingRate, seed, n)
--      testPoisson(stratifiedData, false, samplingRate, seed, n)
--    }
--
--    // Without replacement validation
--    def testBernoulli(stratifiedData: RDD[(String, Int)],
--        exact: Boolean,
--        samplingRate: Double,
--        seed: Long,
--        n: Long): Unit = {
--      val trials = stratifiedData.countByKey()
--      val fractions = Map("1" -> samplingRate, "0" -> samplingRate)
--      val sample = if (exact) {
--        stratifiedData.sampleByKeyExact(false, fractions, seed)
--      } else {
--        stratifiedData.sampleByKey(false, fractions, seed)
--      }
--      val sampleCounts = sample.countByKey()
--      val takeSample = sample.collect()
--      sampleCounts.foreach { case (k, v) =>
--        assertBinomialSample(exact = exact, actual = v.toInt, trials = trials(k).toInt,
--          p = samplingRate)
--      }
--      assert(takeSample.size === takeSample.toSet.size)
--      takeSample.foreach { x => assert(1 <= x._2 && x._2 <= n, s"elements not in [1, $n]") }
--    }
--
--    // With replacement validation
--    def testPoisson(stratifiedData: RDD[(String, Int)],
--        exact: Boolean,
--        samplingRate: Double,
--        seed: Long,
--        n: Long): Unit = {
--      val trials = stratifiedData.countByKey()
--      val expectedSampleSize = stratifiedData.countByKey().mapValues(count =>
--        math.ceil(count * samplingRate).toInt)
--      val fractions = Map("1" -> samplingRate, "0" -> samplingRate)
--      val sample = if (exact) {
--        stratifiedData.sampleByKeyExact(true, fractions, seed)
--      } else {
--        stratifiedData.sampleByKey(true, fractions, seed)
--      }
--      val sampleCounts = sample.countByKey()
--      val takeSample = sample.collect()
--      sampleCounts.foreach { case (k, v) =>
--        assertPoissonSample(exact, actual = v.toInt, trials = trials(k).toInt, p = samplingRate)
--      }
--      val groupedByKey = takeSample.groupBy(_._1)
--      for ((key, v) <- groupedByKey) {
--        if (expectedSampleSize(key) >= 100 && samplingRate >= 0.1) {
--          // sample large enough for there to be repeats with high likelihood
--          assert(v.toSet.size < expectedSampleSize(key))
--        } else {
--          if (exact) {
--            assert(v.toSet.size <= expectedSampleSize(key))
--          } else {
--            assertPoissonSample(false, actual = v.toSet.size, trials(key).toInt, p = samplingRate)
--          }
--        }
--      }
--      takeSample.foreach(x => assert(1 <= x._2 && x._2 <= n, s"elements not in [1, $n]"))
--    }
--  }

end)
