<img align="right" src="stuart.png" width="70">

# Contributing to Stuart

## Guidelines

* [Busted](https://olivinelabs.com/busted/)-based [TDD](https://en.wikipedia.org/wiki/Test-driven_development)
* Class modules begin with an uppercase letter, and end up in their own file that begins with an uppercase letter (e.g. `RDD.lua`)
* Two spaces for indents.
* The `_` global variable is the unused variable stand-in.
* Companion libraries such as [Stuart ML](https://github.com/BixData/stuart-ml) (a Lua port of Spark MLlib) will end up in their own separate Git repo and [LuaRocks module](http://luarocks.org/modules/drauschenbach/stuart-ml).

## Where to Start

Here are some areas where contributors are welcome, in order of their value to the project.

### 1. Port more Apache Spark examples to Lua

Apache Spark example programs for Spark and Spark Streaming exist at [examples/src/main/java/org/apache/spark/examples](https://github.com/apache/spark/tree/v2.2.0/examples/src/main/java/org/apache/spark/examples) and [examples/src/main/java/org/apache/spark/examples/streaming](https://github.com/apache/spark/tree/v2.2.0/examples/src/main/java/org/apache/spark/examples/streaming).

Once ported to Stuart, they exist in this project at [examples/ApacheSpark](https://github.com/BixData/stuart/tree/master/examples/ApacheSpark).

These programs would validate the basic utility of the platform.

### 2. Contribute to supporting libraries

Stuart module dependencies are created in pure Lua so that amalgamated Lua scripts can transpile to C or run on diverse VMs. This places a burden on development of libraries that support Stuart, Stuart ML, and Stuart SQL.

* [lua-parquet](https://github.com/BixData/lua-parquet) - needs significant additional support for RLE, and other codecs such as [Brotli](https://github.com/BixData/lua-brotli), `gzip`, `lzo`, and `snappy`.
* [lua-long](https://github.com/BixData/lua-long) - needs Lua 5.3 support, and stringify hangs for many numbers.
* [vstruct](https://luarocks.org/modules/deepakjois/vstruct) - needs Lua 5.3 support.
* [stuart-ml](https://github.com/BixData/stuart-ml) - KMeans Clustering was the first algorithm to be ported, but Spark ML contains dozens more Classification and Regression models.
* [stuart-sql](https://github.com/BixData/stuart-sql) - DataFrame support, among other things.

### 3. Fix Issues

Issues are filed at [github.com/BixData/stuart/issues](https://github.com/BixData/stuart/issues).
