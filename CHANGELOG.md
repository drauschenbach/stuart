## [2.0.4] - 2019-04-20
### Changed
- Ensure RDD:first() raises error when RDD is empty; expected by Stuart ML RowMatrix:numCols()

## [2.0.3] - 2019-03-08
### Added
- [#129](https://github.com/BixData/stuart/issues/129) Support RDD:sortBy() lexicographic comparison for multiple fields

## [2.0.2] - 2019-02-23
### Changed
- Published new [amalg-redis](https://luarocks.org/modules/drauschenbach/amalg-redis) module to streamline Redis amalgamation instructions at [examples/redis](./examples/redis/)
- Document how to run Stuart test suites within Redis at [spec-redis](./spec-redis/)

### Fixed
- [#125](https://github.com/BixData/stuart/issues/125) RDD:combineByKey() ignores createCombiner argument

## [2.0.0] - 2018-12-16
### Added
- [#117](https://github.com/BixData/stuart/issues/117) Demonstrate how to use Stuart in Redis

### Changed
- `stuart.util` split() now returns an empty string as a first result element for strings that start with the separator char, making it usable for path joining

### Fixed
- [#115](https://github.com/BixData/stuart/issues/115) Cannot specify an absolute path using Hadoop FileSystem abstraction
- [#119](https://github.com/BixData/stuart/issues/119) Three levels of inheritance causes infinite loop in constructor
- [#121](https://github.com/BixData/stuart/issues/121) SocketReceiver returns no data
- [#123](https://github.com/BixData/stuart/issues/123) StreamingContext throws error when no data is received

## [1.0.1] - 2018-11-17
### Added
- [#113](https://github.com/BixData/stuart/issues/113) Use the 'url' native module when present

### Changed
- Class framework drastically simplified by adopting Microlight

### Fixed
- [#107](https://github.com/BixData/stuart/issues/107) WindowedDStream was left out of the LuaRocks packaging
- [#111](https://github.com/BixData/stuart/issues/111) Subclass cannot call a superclass constructor that was not explicitly provided

## [1.0.0] - 2018-11-08
### Added
- [#96](https://github.com/BixData/stuart/issues/96) Export Logger level consts for use in calls to setLevel()

### Changed
- The `stuart.util.class` module has been renamed to `stuart.class`, which is more 1-1 with Torch
- The `stuart.util.isInstanceOf()` function has been retired, replaced by `stuart.class.istype()` or `stuart.istype()`
- Reinstated middleclass class framework to fix indexing in Stuart ML, while retaining most of the Torch-style function calls

### Fixed
- [#98](https://github.com/BixData/stuart/issues/98) Trace and Warn logging fails trying to call a nil function
- [#100](https://github.com/BixData/stuart/issues/100) Replace use of class() with class.new() because use of metatables within LTR Romtables is unknown (eLua interop)

## [0.2.0] - 2018-11-03
### Changed
- [#94](https://github.com/BixData/stuart/issues/94) Replace use of middleclass with a Torch-compatible equivalent that is easier to represent in an eLua LTR ROM-based module table (eLua interop)

## [0.1.9] - 2018-11-02
### Changed
- [#89](https://github.com/BixData/stuart/issues/89) Removed all use of coroutines, allowing transpiling of Spark Streaming to C (eLua interop)
- [#92](https://github.com/BixData/stuart/issues/92) Remove all use of module upvalues, so that modules can be more easily transpiled to C then burned into ROM and chipsets (eLua [LTR](http://www.eluaproject.net/doc/v0.9/en_arch_ltr.html) interop)

## [0.1.8] - 2018-10-14
### Added
- Added Fengari to list of supported VMs
- New examples/web demonstrates Spark Pi example running in a browser using the Fengari VM
- [#15](https://github.com/BixData/stuart/issues/15) Support `DStream` countByWindow(), map(), reduce(), and window()
- [#71](https://github.com/BixData/stuart/issues/71) Package and deploy releases to npmjs.com and jsDelivr as [lua-stuart](https://www.npmjs.com/package/lua-stuart)
- [#82](https://github.com/BixData/stuart/issues/82) Support `DStream` foreachRDD() timestamp 2nd argument
- Support `RDD` treeAggregate() and treeReduce()

### Changed
- Removed `stuart.util.mosesPatchedRange`, fixed in Moses 2.0.0
- `RDD` aggregate() and treeAggregate() now have the ability to clone a complex zeroValue by calling a clone() function when present

### Fixed
- [#73](https://github.com/BixData/stuart/issues/73) github.com/fengari-lua/fengari-web interop fails because Stuart assumes an io module is always present for logging
- [#75](https://github.com/BixData/stuart/issues/75) Context\_spec.lua ignores WEBHDFS_URL
- [#76](https://github.com/BixData/stuart/issues/76) RDD:foreach() modifies RDD

## [0.1.7] - 2018-09-15
### Added
- Dockerfiles provided for testing with specific versions of Lua 5.1, 5.2, 5.3

### Changed
- [#68](https://github.com/BixData/stuart/issues/68) Upgrade to Moses 2.1.0 (from 1.6.1), which now has cleaner function chaining semantics that more closely resembles lodash/underscore
- Moses is no longer embedded, and is now once again a LuaRocks dependency. Embedding a trimmed down version did not help with eLua support, which instead requires transpiling modules to C to remove memory pressure

### Fixed
- [#66](https://github.com/BixData/stuart/issues/66) Travis CI builds fail due to new release of Moses 2.x
- [#64](https://github.com/BixData/stuart/issues/64) `Context:hadoopFile()` error reading a directory containing a nested directory
- [#2](https://github.com/BixData/stuart/issues/2) La Trobe Univ RDD `stdev()` unit test fails

## [0.1.6] - 2017-12-31
### Added
- Support [eLua](http://www.eluaproject.net).
- [#43](https://github.com/BixData/stuart/issues/43) Support for `Context:textFile()` on a directory. Makes use of `luafilesystem` module for local filesystem testing, when present. Supports `webhdfs:` URLs.
- New `stuart.interface.sleep` module can be preloaded with a function that sleeps to prevent pegging the CPU in multithreaded environments. Defaults to LuaSocket sleep() when present.

### Changed
- Dropped formal LuaSocket dependency. It is used when present, like `cjson`, but no longer required. This change is required for eLua support.
- Dropped formal moses dependency, and instead embed a copy that is trimmed of unused functions (~27% reduction).
- Reduced memory usage due to JSON decoding by directly using lunajson's decode module instead of its parent module which references other unused features.
- Removed mandatory dependence on `os` module, since it does not exist in eLua environments.
- Don't reference unused Spark Streaming modules from Spark Pi, which bloats the amalg cache and generated eLua image.
- Defer loading of WebHdfsFileSystem or LocalFileSystem modules until they are used, so that they don't bloat the alamg cache and generated eLua image.

### Fixed
- `fileSystemFactory`, `StreamingContext`, and `WebHdfsFileSystem` modules failed to load in an eLua environment, where LuaSocket is not present.

## [0.1.5-1] - 2017-12-11
### Added
- New Hadoop `Path` class, which introduces new [net-url](https://luarocks.org/modules/golgote/net-url) module dependency

### Fixed
- [#40](https://github.com/BixData/stuart/issues/40) `util.isInstanceOf` fails for non-table arguments such as nil

## [0.1.4] - 2017-11-27
### Added
- New `stuart.interface.clock` module that can be preloaded with a custom implementation that binds Stuart to a proprietary hardware clock, instead of always depending on LuaSocket for time which may be unavailable in microcontroller environments
- Support `Context` `stop()` and `isStopped()`, and `StreamingContext` `stop(stopSparkContext)` param
- [#9](https://github.com/BixData/stuart/issues/9) Support `SparkConf` class
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
- [#26](https://github.com/BixData/stuart/issues/26) `RDD:takeSample()` fails to return any results when RDD contains middleclass classes

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
