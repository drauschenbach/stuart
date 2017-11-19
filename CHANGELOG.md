## [unreleased]
### Added
- Support `Context` `stop()` and `isStopped()`, and `StreamingContext` `stop(stopSparkContext)` param
- Support `SparkConf` class
- Support `logging` module and `Logger` class, and add logging to RDD, Context, DStream, and Receiver classes. Connect/disconnect info now shown.

## [0.1.3] - 2017-11-11
### Added
- Support `Context` `defaultParallelism` field (defaults to 1)
- Support `RDD:groupByKey()` `numPartitions` param

### Changed
- Consolidate Apache Spark and Stuart unit tests into a single unified folder hierarchy
- Renamed assertions within tolerance to `assert_relTol` and `assert_absTol`, which is more 1-1 with Spark Scala unit tests

## [0.1.2] - 2017-10-28
### Added
- Support `RDD:sample()` with an initial implementation that does not yet respect the `withReplacement` param
- Support `RDD:sum()` and `RDD:sumApprox()`
- Support `RDD:toString()` and implicit `__tostring` stringification of RDDs for debugging
- Ported Apache Spark `SparkPi` example
- Travis-based Luacheck source code static analysis now also applies to specs

### Fixed
- `RDD:takeSample()` fails to return any results when RDD contains middleclass classes

### Changed
- Support random seed 3rd argument to `RDD:takeSample()`

## [0.1.1] - 2017-10-14
### Added
- Use `luacjson`, when available (but not required), for faster JSON parsing
- Support `StreamingContext:awaitTermination()`
- Support `QueueInputDStream` `oneAtATime` mode
- Support `DStream:groupByKey()`
- Travis-based continuous integration on LUA 5.1, 5.2, and 5.3, and LuaJIT 2.0 and 2.1
- Ported Apache Spark `BasicOperationsSuite` test coverage for `DStream:count()`

### Fixed
- Remove use of `lodash` from unit tests because of Lua 5.1 incompatibility
- `ReceiverInputDStream` module leakage into `DStream` module
- A `NewStreamingContext()` constructor variant was broken
- `NewContext()` constructor was missing support for passing master and appname params
- Several local variable leaks into the global namespace
- A memory leak in `SocketReceiver` due to misnamed variable reference

### Changed
- Organize specs according to module hierarchy
- Make cooperative multitasking context switch period match the `StreamingContext` batch duration

## [0.1.0] - 2017-09-30
### Added
- Support `Context` class with emptyRDD(), hadoopFile(), makeRDD(), parallelize(), textFile(), and union() support
- Support `Partition` class
- Support `RDD` class with aggregate(), aggregateByKey(), cache(), cartesian(), coalesce(), collect(), collectAsMap(), combineByKey(), count(), countApprox(), countByKey(),
countByValue(), distinct(), filter(), filterByRange(), first(), flatMap(), flatMapValues(), fold(), foldByKey(), foreach(), foreachPartition(), glom(), groupBy(), groupByKey(), histogram(), intersection(), isEmpty(), join(), keyBy(), keys(), leftOuterJoin(), lookup(), map(), mapPartitions(), mapPartitionsWithIndex(), mapValues(), max(), mean(), meanApprox(), min(), reduce(), reduceByKey(), repartition(), rightOuterJoin(), setName(), sortBy(), sortByKey(), stats(), stdev(), subtract(), subtractByKey(), take(), takeSample(), toLocalIterator(), top(), union(), values(), zip(), and zipWithIndex() support
- Support `StreamingContext` class with cooperative multitasking support for multiple concurrent receivers, with awaitTerminationOrTimeout(), getState(), queueStream(), receiverStream(), socketTextStream(), start(), and stop() support
- Support `DStream` class with count(), foreachRDD(), mapValues(), start(), stop(), and transform() support
- Support `SocketInputDStream`, `QueueInputDStream`, and `TransformedDStream` classes
- Support `Receiver`, `SocketReceiver`, and `ReceiverInputDStream` classes
- Provide an `HttpReceiver` class that supports http chunked streaming endpoints
- Support WebHDFS URLs
- LuaRocks packaging

<small>(formatted per [keepachangelog-1.1.0](http://keepachangelog.com/en/1.0.0/))</small>
