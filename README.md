# Stuart

<img src="http://downloadicons.net/sites/default/files/mouse-icon-86497.png" width="100">

(He's little). A native Lua implementation of [Apache Spark 2.2.0](https://spark.apache.org/docs/2.2.0/), designed for embedding and edge computing.

![Build Status](https://api.travis-ci.org/BixData/stuart.svg?branch=master)

## Getting Started

### Installing

```bash
$ luarocks install stuart
```

### Reading a text file

Create a "Stuart Context", then count the number of lines in this README:

```lua
$ lua
Lua 5.2.4  Copyright (C) 1994-2015 Lua.org, PUC-Rio

sc = require 'stuart'.NewContext() 
rdd = sc:textFile('README.md')
print(rdd:count())
151
```

### Working with lists of values

```lua
rdd = sc:parallelize({1,2,3,4,5,6,7,8,9,10}, 3)
filtered = rdd:filter(function(x) return x % 2 == 0 end)
print('evens: ' .. table.concat(filtered:collect(), ','))
evens: {2,4,6,8,10}
```

### Working with lists of pairs

```lua
rdd = sc:parallelize({{4,'Gnu'}, {4,'Yak'}, {5,'Mouse'}, {4,'Dog'}})
countsByKey = rdd:countByKey()
print(countsByKey[4])
3
print(countsByKey[5])
1
```

### Streaming with a socket text datasource

Start a local server with netcat:

```bash
$ nc -lk 9999
```

Start a Spark Streaming job to read from the netcat server:

```lua
sc = require 'stuart'.NewContext()
ssc = require 'stuart'.NewStreamingContext(sc, 0.5)

dstream = ssc:socketTextStream('localhost', 9999)
dstream:foreachRDD(function(rdd)
	print('Received RDD: ' .. rdd:collect())
end)
ssc:start()
ssc:awaitTerminationOrTimeout(10)
ssc:stop()
```

Then type some input into the netcat server:

```
abc
123
```

### Streaming with a custom receiver

This custom receiver acts like a `SocketInputDStream`, and reads lines of text from a socket.

```lua
local class = require 'middleclass'
local socket = require 'socket'
local stuart = require 'stuart'
local Receiver = require 'stuart.streaming.Receiver'

-- MyReceiver ------------------------------

local MyReceiver = class('MyReceiver', Receiver)

function MyReceiver:initialize(ssc, hostname, port)
  Receiver.initialize(self, ssc)
  self.hostname = hostname
  self.port = port or 0
end

function MyReceiver:onStart()
  self.conn = socket.connect(self.hostname, self.port)
end

function MyReceiver:onStop()
	if self.conn ~= nil then self.conn:close() end
end

function MyReceiver:run(durationBudget)
  local timeOfLastYield = socket.gettime()
  local rdds = {}
  local minWait = 0.02 -- never block less than 20ms
  while true do
    local elapsed = socket.gettime() - timeOfLastYield
    if elapsed > durationBudget then
      coroutine.yield(rdds)
      rdds = {}
      timeOfLastYield = socket.gettime()
    else
      self.conn:settimeout(math.max(minWait, durationBudget - elapsed))
      local line, err = self.conn:receive('*l')
      if not err then
        rdds[#rdds+1] = self.ssc.sc:makeRDD({line})
      end
    end
  end
end

-- Spark Streaming Job ------------------------------

sc = stuart.NewContext()
ssc = stuart.NewStreamingContext(sc, 0.5)

local receiver = MyReceiver:new(ssc, 'localhost', 9999)
local dstream = ssc:receiverStream(receiver)
dstream:foreachRDD(function(rdd)
	print('Received RDD: ' .. rdd:collect())
end)
ssc:start()
ssc:awaitTerminationOrTimeout(10)
ssc:stop()
```

## Dependencies

* [LuaSocket](https://luarocks.org/modules/luarocks/luasocket), where networking or system time are required.
* [lunajson](https://luarocks.org/modules/grafi/lunajson), the pure-Lua JSON parser. Used in WebHDFS response parsing. If the [cjson](https://luarocks.org/modules/luarocks/lua-cjson) module is detected, it is used first for performance. But otherwise Lunajson is the portable fall-back.
* [middleclass](https://luarocks.org/modules/kikito/middleclass) to streamline inheritance and allow for literal adaptation of many Apache Spark APIs.
* [moses](https://luarocks.org/modules/yonaba/moses), the underscore-inspired Lua-optimized workhorse.

## Compatibility

Stuart is compatible with:

* [GopherLua](https://github.com/yuin/gopher-lua)
* [Lua 5.1+](https://www.lua.org)
* [LuaJIT](https://www.lua.org)

Use [gluasocket](https://github.com/BixData/gluasocket) to embed Stuart in a Go app.

Stuart is incompatible with:

* [Shopify/go-lua](https://github.com/Shopify/go-lua), due to its lack of `coroutine` and `debug.getinfo()` capabilities.

## Roadmap Brainstorm

* Support [eLua Boards](http://wiki.eluaproject.net/Boards) and their alternative I/O and clock mechanisms
* Support [DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html)
* Support [MLlib Import](https://spark.apache.org/mllib/) in a companion project. Load a model, and use it at the edge, perhaps from a Spark Streaming control loop.
* Support [PMML Import](https://spark.apache.org/docs/2.2.0/mllib-pmml-model-export.html) in a companion project.
* A Redis scheduler that partitions RDDs across Redis servers, and sends Lua closures into Redis.

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
* The `_` global variable is the unused variable stand-in.
* Companion libraries such as Stuart ML (a Lua port of Spark ML) will end up in their own separate Git repo and LuaRocks module such as `"stuart-ml"`.

## Building

The LuaRocks built-in build system is used for packaging.

```bash
$ luarocks make rockspecs/stuart-<version>.rockspec
```

## Testing

Testing with `lua-cjson`:

```
$ luarocks install busted
$ luarocks install lua-cjson
$ busted
●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●
129 successes / 0 failures / 0 errors / 0 pending : 10.895618 seconds
```

Testing with `lunajson`:

```
$ luarocks remove lua-cjson
$ busted
●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●
129 successes / 0 failures / 0 errors / 2 pending : 10.895618 seconds

Pending → ...
util.json can decode a scalar using cjson
... cjson not installed

Pending → ...
util.json can decode an object using cjson
... cjson not installed
```
