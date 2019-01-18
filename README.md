<img align="right" src="stuart.png" width="70">

## Stuart

(He's little). A pure Lua rewrite of [Apache Spark 2.2](https://spark.apache.org/docs/2.2.0/), designed for embedding and edge computing.

![Build Status](https://api.travis-ci.org/BixData/stuart.svg?branch=master)
[![License](http://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](LICENSE)
[![Lua](https://img.shields.io/badge/Lua-5.1%20|%205.2%20|%205.3%20|%20JIT%202.0%20|%20JIT%202.1%20|%20eLua%20|%20Fengari%20|%20GopherLua%20|%20Redis-blue.svg)]()

### Contents

* [Installation](#installation)
* [Usage](#usage)
	* [Reading a text file](#reading-a-text-file)
	* [Working with lists of values](#working-with-lists-of-values)
	* [Working with lists of pairs](#working-with-lists-of-pairs)
	* [Streaming with a socket text datasource](#streaming-with-a-socket-text-datasource)
	* [Streaming with a custom receiver](#streaming-with-a-custom-receiver)
* [Embedding](#embedding)
* [Compatibility](#compatibility)
* [Libraries for Stuart](#libraries-for-stuart)
* [Design](#design)
	* [Why Spark?](#why-spark)
	* [Why Lua?](#why-lua)
* [Building](#building)
* [Testing](#testing)

## Installation

To install on an operating system:

```bash
$ luarocks install stuart
```

To load into a web page:

```html
<html>
  <body>
    <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/fengari-web@0.1.2/dist/fengari-web.js"></script>
    <script type="application/lua" src="https://cdn.jsdelivr.net/npm/lua-stuart@0.2.0-0/stuart.lua"></script>
  
    <script type="application/lua">
      local stuart = require 'stuart'
      local sc = stuart.NewContext('local[1]', 'My Spark job')
      ...
    </script>
    
  </body>
</html>
```

To use in Redis, see [examples/redis](./examples/redis/):

```
$ redis-cli --eval SparkPi-with-dependencies.lua 0,0
"Pi is roughly 3.1332956664783"
```

## Usage

### Reading a text file

Create a "Stuart Context", then count the number of lines in this README:

```lua
$ lua
Lua 5.2.4  Copyright (C) 1994-2015 Lua.org, PUC-Rio

local stuart = require 'stuart'
local sc = stuart.NewContext()
local rdd = sc:textFile('README.md')
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

Start by installing network support:

```sh
$ luarocks install luasocket
```

Next, start a local network service with netcat:

```bash
$ nc -lk 9999
```

Start a Spark Streaming job to read from the network service:

```lua
local stuart = require 'stuart'
local sc = stuart.NewContext()
local ssc = stuart.NewStreamingContext(sc, 1.5)

local dstream = ssc:socketTextStream('localhost', 9999)
dstream:foreachRDD(function(rdd)
  print('Received:', string.format('%s {%s}', tostring(rdd), table.concat(rdd:collect(),',')))
end)

ssc:start()
ssc:awaitTerminationOrTimeout(10)
ssc:stop()
```

Then type some input into netcat:

```
abc
def
123
```

The output shows lines received within the 1.5 second batch interval:

```
INFO Running Stuart (Embedded Spark 2.2.0)
INFO Connected to localhost:9999
Received RDD:	RDD[1] {abc}
Received RDD:	RDD[2] {def,123}
```

### Streaming with a custom receiver

This custom receiver acts like a `SocketInputDStream`, and reads lines of text from a socket.

```lua
local now = require 'stuart.interface'.now
local Receiver = require 'stuart.streaming.Receiver'
local socket = require 'socket'
local stuart = require 'stuart'

-- MyReceiver ------------------------------

local MyReceiver = stuart.class(Receiver)

function MyReceiver:_init(ssc, hostname, port)
  Receiver._init(self, ssc)
  self.hostname = hostname
  self.port = port or 0
end

function MyReceiver:onStart()
  self.conn = socket.connect(self.hostname, self.port)
end

function MyReceiver:onStop()
  if self.conn ~= nil then self.conn:close() end
end

function MyReceiver:poll(durationBudget)
  local startTime = now()
  local data = {}
  local minWait = 0.02
  while true do
    local elapsed = now() - startTime
    if elapsed > durationBudget then break end
    self.conn:settimeout(math.max(minWait, durationBudget - elapsed))
    local line, err = self.conn:receive('*l')
    if not err then
      data[#data+1] = line
    end
  end
  return self.ssc.sc:makeRDD(data)
end

-- Spark Streaming Job ------------------------------

sc = stuart.NewContext()
ssc = stuart.NewStreamingContext(sc, 0.5)

local receiver = MyReceiver.new(ssc, 'localhost', 9999)
local dstream = ssc:receiverStream(receiver)
dstream:foreachRDD(function(rdd)
  print('Received RDD: ' .. rdd:collect())
end)
ssc:start()
ssc:awaitTerminationOrTimeout(10)
ssc:stop()
```

## Embedding

The `stuart.interface` module provide interfaces to hardware or a host OS, designed to make it easy for you to preload your own custom module that is specific to your host application or device.

To embed Stuart into a Go app, use:

* [gluabit32](https://github.com/BixData/gluabit32)
* [gluasocket](https://github.com/BixData/gluasocket)

To embed Stuart into an eLua image, see [stuart-elua](https://github.com/BixData/stuart-elua).

See the [stuart-hardware](https://github.com/BixData/stuart-hardware) project for edge hardware specific integration guides.

## Compatibility

Stuart is compatible with:

* [eLua](http://www.eluaproject.net) (a C-based 5.1 baremetal VM that runs on a breadth of microcontroller families in as little as 32k of RAM)
* [Fengari](https://github.com/fengari-lua/fengari) (a JavaScript-based Lua 5.3 VM)
* [GopherLua](https://github.com/yuin/gopher-lua) (a Go-based Lua 5.1 VM)
* [Lua](https://www.lua.org) 5.1, 5.2, 5.3
* [LuaJIT](https://www.lua.org) 2.0, 2.1
* [Redis](https://redis.io/commands/eval) 2.6+ (a Lua 5.1 VM)

## Libraries for Stuart

* [stuart-ml](https://github.com/BixData/stuart-ml) : A Lua port of [Spark MLlib](https://spark.apache.org/docs/2.2.0/ml-guide.html)
* [stuart-sql](https://github.com/BixData/stuart-sql) : A Lua port of [Spark SQL](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html)
* [stuart-redis](https://github.com/BixData/stuart-redis) : Extended Redis support

## Roadmap

* Improve streaming support and test coverage
* Build out Stuart ML to support more models
* Build out Stuart SQL to support DataFrames
* Improve Stuart SQL's Parquet interop with Lua 5.3 support and more codecs

## Design

Stuart is designed for real-time and embedding, and so it follows some rules:

* It does not perform deferred evaluation of anything; all compute costs are paid upfront for predictable throughput.
* It uses pure Lua and does not include native C code. This maximizes portability and opportunity to be cross-compiled. Any potential C code optimizations are externally sourced through the module loader. For example, Stuart links to `lunajson`, but it also detects and uses `cjson` when that native module is present.
* It does not execute programs (like `ls` or `dir` to list files), because there may not even be an OS.
* It does not make use of coroutines, in order to ensure easy transpiling to C.
* It does not use upvalues or metatables in module scripts, so that module tables can be burned into ROM and chipsets (see [eLua LTR](http://www.eluaproject.net/doc/v0.9/en_arch_ltr.html))
* It should be able to eventually do everything that [Apache Spark](https://spark.apache.org) does.

### Why Spark?

While many frameworks deliver streaming analytics capabilities, Spark leads the pack in numbers of trained data scientists, numbers of SaaS environments where Spark models can be built and trained, numbers of contributors moving the platform forward, numbers of universities teaching it, and net commercial investment.

### Why Lua?

**Depoyment.** Amalgamated Lua jobs with inlined module dependencies solves the Spark job deployment problem, and obviates the need for any shared filesystem or brittle classpath coordination. [Redis Scripting](https://redis.io/commands/eval) showcases the power of SHA1 content hashing for Lua job distribution.

**Packaging.** Lua jobs, like JavaScript, are easy to minify, and statically analyze to strip out unused modules and function calls. Your job script only need be as large as the number of Spark capabilities it makes use of.

**Portability.** Because Lua is a tiny language that elegantly supports classes and closures, it serves as a better source of truth for functional algorithms than Scala. This makes it relatively easy for Stuart jobs to be transpiled into Scala, Java, Python, Go, C, or maybe even CUDA, or to be interpreted by a VM in any of those same environments, which significantly extends Spark's reach by divorcing it from the JVM.

**Embedding.** Lua is arguably one of the most crash-proof language runtimes, making it attractive for industrial automation, sensors, wearables, and microcontrollers. Whereas JVM-based analytics tend to require an operator.

**GPUs.** If you are thinking about pushing closures into a GPU, Lua seems like a reasonable choice, and one of the easier languages to transpile into OpenCL or CUDA.

**Torch.** [Torch](http://torch.ch) is the original deep-learning library ecosystem, 15+ years mature, and with deep ties to university and leading commercial interests. It runs on mobile phones, and serves as a fantastic case in point for why Lua makes sense for analytics jobs. A data scientist should be able to use Spark and Torch side-by-side, and maybe even from the same Spark Streaming control loop.

## Building

The LuaRocks built-in build system is used for packaging.

```bash
$ luarocks make stuart-<version>.rockspec
stuart <version> is now built and installed in /usr/local (license: Apache 2.0)
```

## Testing

Testing with `lua-cjson`:

```sh
$ luarocks install busted
$ luarocks install lua-cjson
$ luarocks intall moses
$ busted -v --defer-print
17/11/12 08:46:51 INFO Running Stuart (Embedded Spark) version 2.2.0
...
177 successes / 0 failures / 0 errors / 0 pending : 8.026833 seconds
```

Testing with `lunajson`:

```sh
$ luarocks remove lua-cjson
$ busted -v --defer-print
17/11/12 08:46:51 INFO Running Stuart (Embedded Spark) version 2.2.0
...
175 successes / 0 failures / 0 errors / 2 pending : 8.026833 seconds

Pending → ...
util.json can decode a scalar using cjson
... cjson not installed

Pending → ...
util.json can decode an object using cjson
... cjson not installed
```

Testing with a WebHDFS endpoint:

```sh
$ WEBHDFS_URL=webhdfs://localhost:50075/webhdfs busted -v --defer-print
```

### Testing with a Specific Lua Version

Various Dockerfiles are made available in the root directory to provide a specific Lua VM for the test suite:

* `Test-Lua5.1.Dockerfile`
* `Test-Lua5.2.Dockerfile`
* `Test-Lua5.3.Dockerfile`

```sh
$ docker build -f Test-Lua5.3.Dockerfile -t test .
$ docker run -it test busted -v --defer-print
●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●
172 successes / 0 failures / 0 errors / 5 pending : 10.246418 seconds
```
