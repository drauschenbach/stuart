# Stuart

<img src="http://downloadicons.net/sites/default/files/mouse-icon-86497.png" width="100">

(He's little). A native Lua implementation of [Apache Spark](https://spark.apache.org), designed for embedding and edge computing.

## Getting Started

### Reading a text file

Create a "Stuart Context", then count the number of lines in this README:

```lua
$ lua
Lua 5.2.4  Copyright (C) 1994-2015 Lua.org, PUC-Rio

sc = require 'stuart'.NewContext() 
rdd = sc:textFile('README.md')
print(rdd:count())
41
```

### Streaming

Per Lua conventions, all durations are specified in seconds.

```lua
sc = require 'stuart'.NewContext()
ssc = require 'stuart'.NewStreamingContext(sc, 0.5)

rdd1 = sc:parallelize({'dog', 'cat'})
rdd2 = sc:parallelize{{'mouse', 'rabbit'})
rdds = {rdd1,rdd2}
dstream = ssc:queueStream(rdds)
dstream:foreachRDD(function(rdd)
	print('Received RDD: ' .. rdd)
end)

ssc:awaitTerminationOrTimeout(1.5)
```

### Working with lists of values

```lua
rdd = sc:parallelize({1,2,3,4,5,6,7,8,9,10}, 3)
filtered = rdd:filter(function(x) return x % 2 == 0 end)
assert.same({2,4,6,8,10}, filtered:collect())
```

### Working with lists of pairs

```lua
rdd = sc:parallelize({{4,'Gnu'}, {4,'Yak'}, {5,'Mouse'}, {4,'Dog'}})
countsByKey = rdd:countByKey()
assert.equals(3, countsByKey[4])
assert.equals(1, countsByKey[5])
```

## Requirements

* [LuaSocket](https://luarocks.org/modules/luarocks/luasocket), where networking or system time are required.
* [lunajson](https://luarocks.org/modules/grafi/lunajson), the pure-Lua JSON parser. Used in WebHDFS response parsing.
* [middleclass](https://luarocks.org/modules/kikito/middleclass) to streamline inheritance and allow for literal adaptation of many Apache Spark APIs.
* [moses](https://luarocks.org/modules/yonaba/moses), the underscore-inspired Lua-optimized workhorse.

## Compatibility

Stuart is compatible with:

* [GopherLua](https://github.com/yuin/gopher-lua)
* [Lua 5.1+](https://www.lua.org)
* [LuaJIT](https://www.lua.org)

Use [gluasocket](https://github.com/BixData/gluasocket) to embed Stuart Streaming in a Go binary.

Stuart is incompatible with:

* [Shopify/go-lua](https://github.com/Shopify/go-lua), due to its lack of `coroutine` and `debug.getinfo()` capabilities.

## Roadmap Brainstorm

* Local in-memory [RDDs](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.rdd.RDD) and [DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).
* [Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html) capabilities.
* [MLlib](https://spark.apache.org/mllib/) support. Load a model, and use it at the edge, perhaps from a Spark Streaming control loop.
* A Redis scheduler. RDDs partitioned across Redis servers. Lua closures sent into Redis to run.

## Design

Stuart is designed for embedding, and so follows some rules:

* It uses pure Lua and does not include native C code. This maximizes portability and opportunity to be interpreted by a JIT or cross-compiler. Any potential C code optimizations are externally sourced through the Lua module loader.
* It does not execute programs (like `ls` or `dir` to list files); there may not even be an OS.
* It should be able to eventually do everything that [Apache Spark](https://spark.apache.org) does.

### Contributor Guidelines

* [Busted](https://olivinelabs.com/busted/)-based [TDD](https://en.wikipedia.org/wiki/Test-driven_development)
* Class modules begin with an uppercase letter, and end up in their own file that begins with an uppercase letter (e.g. `RDD.lua`)
* Modules begin with a lowercase letter (e.g. `stuart.lua`, `fileSystemFactory.lua`)
* Two spaces for indents.
* Companion libraries such as Stuart ML (a Lua port of Spark ML) will end up in their own separate Git repo and LuaRocks module `"stuart-ml"`. This means all sources can be found in the root folder, which reserves subdirectories for specs, their fixtures, and examples.

## Testing

```
$ luarocks install busted
$ busted
●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●
110 successes / 0 failures / 0 errors / 0 pending : 8.477593 seconds
```
