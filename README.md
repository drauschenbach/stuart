# Stuart

<img src="http://downloadicons.net/sites/default/files/mouse-icon-86497.png" width="100">

(He's little). A native Lua implementation of [Apache Spark 2.2.0](https://spark.apache.org/docs/2.2.0/), designed for embedding and edge computing.

![Build Status](https://api.travis-ci.org/BixData/stuart.svg?branch=master)

### Contents

* [Installation](#installation)
* [Usage](#usage)
	* [Reading a text file](#reading-a-text-file)
	* [Working with lists of values](#working-with-lists-of-values)
	* [Working with lists of pairs](#working-with-lists-of-pairs)
	* [Streaming with a socket text datasource](#streaming-with-a-socket-text-datasource)
	* [Streaming with a custom receiver](#streaming-with-a-custom-receiver)
* [Embedding](#embedding)
* [Dependencies](#dependencies)
* [Compatibility](#compatibility)
* [Libraries for Stuart](#libraries-for-stuart)
* [Design](#design)
	* [Why Spark?](#why-spark)
	* [Why Lua?](#why-lua)
* [Contributing](#contributing)
* [Building](#building)
* [Testing](#testing)

## Installation

```bash
$ luarocks install stuart
```

## Usage

### Reading a text file

Create a "Stuart Context", then count the number of lines in this README:

```lua
$ lua
Lua 5.2.4  Copyright (C) 1994-2015 Lua.org, PUC-Rio

local SparkContext = require 'stuart.Context'
local sc = SparkContext:new() 
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

Start a local server with netcat:

```bash
$ nc -lk 9999
```

Start a Spark Streaming job to read from the netcat server:

```lua
local sc = require 'stuart'.NewContext()
local ssc = require 'stuart'.NewStreamingContext(sc, 0.5)

local dstream = ssc:socketTextStream('localhost', 9999)
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

## Embedding

Modules named `stuart.interface.*` provide interfaces to hardware or a host OS, designed to make it easy for you to preload your own custom module that is specific to your host application or device. These interfaces are seen as a public API, and so any changes to them will increment the SemVer versioning accordingly.

### stuart.interface.clock

Used to measure time, which is required by the `StreamingContext` cooperative multitasking. On an OS, implementation defaults to LuaSocket `gettime()` with 4 decimals of precision. Falls back on Lua `os.time(os.clock('*t'))` with 0 digits of precision (whole seconds).

### stuart.interface.sleep

Function used to sleep, when all receivers don't use their full timeslice allotments. Used to prevent pegging the CPU on systems where that makes sense, such as a host OS.

## Dependencies

### Required Dependencies

All required dependencies are pure Lua:

* [lunajson](https://luarocks.org/modules/grafi/lunajson) JSON parser.
* [middleclass](https://luarocks.org/modules/kikito/middleclass) to streamline inheritance and literal adaptation of many Apache Spark classes.
* [moses](https://luarocks.org/modules/yonaba/moses), the underscore-inspired Lua-optimized workhorse.
* [net-url](https://luarocks.org/modules/golgote/net-url), a URL and query string parser/builder/normalizer.

### Optional Dependencies

The following modules are used when present:

* [lua-cjson](https://luarocks.org/modules/openresty/lua-cjson), for higher performance JSON parsing.
* [luasocket](https://luarocks.org/modules/luarocks/luasocket), for networking, system time, and sleep capabilities in operating system environments.

## Compatibility

Stuart is compatible with:

* [eLua](http://www.eluaproject.net) (aka "Embedded Lua"), a 5.1 baremetal VM for microcontrollers
* [GopherLua](https://github.com/yuin/gopher-lua)
* [Lua](https://www.lua.org) 5.1, 5.2, 5.3
* [LuaJIT](https://www.lua.org) 2.0, 2.1

See the [stuart-hardware](https://github.com/BixData/stuart-hardware) project for edge hardware specific integration guides.

## Libraries for Stuart

* [stuart-sql](https://github.com/BixData/stuart-sql) : A Lua port of [Spark SQL](https://spark.apache.org/docs/2.2.0/sql-programming-guide.html), for support of DataFrames and Parquet files
* [stuart-ml](https://github.com/BixData/stuart-ml) : A Lua port of [Spark MLlib](https://spark.apache.org/docs/2.2.0/ml-guide.html), for loading and evaluating models such as `KMeansModel`

To embed Stuart into a Go app, use:

* [gluabit32](https://github.com/BixData/gluabit32)
* [gluasocket](https://github.com/BixData/gluasocket)

## Roadmap

* Support [eLua Boards](http://wiki.eluaproject.net/Boards) by supporting user-defined modules for I/O and clock mechanisms
* Support [PMML Import](https://spark.apache.org/docs/2.2.0/mllib-pmml-model-export.html) via a `stuart-pmml` companion library
* Support a Redis scheduler that partitions RDDs across Redis servers, and sends Lua closures into Redis for execution.
* Support [OpenCL](https://en.wikipedia.org/wiki/OpenCL) or [CUDA](https://en.wikipedia.org/wiki/CUDA) schedulers that send Lua closures into a GPU for execution.

## Design

Stuart is designed for real-time and embedding, and so it follows some rules:

* It does not perform deferred evaluation of anything; all compute costs are paid upfront for predictable throughput.
* It uses pure Lua and does not include native C code. This maximizes portability and opportunity to be cross-compiled. Any potential C code optimizations are externally sourced through the module loader. For example, Stuart links to `lunajson`, but it also detects and uses `cjson` when that native module is present.
* It does not execute programs (like `ls` or `dir` to list files), because there may not even be an OS.
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

## Contributing

### Contribution Guidelines

* [Busted](https://olivinelabs.com/busted/)-based [TDD](https://en.wikipedia.org/wiki/Test-driven_development)
* Class modules begin with an uppercase letter, and end up in their own file that begins with an uppercase letter (e.g. `RDD.lua`)
* Modules begin with a lowercase letter (e.g. `stuart.lua`, `fileSystemFactory.lua`)
* Two spaces for indents.
* The `_` global variable is the unused variable stand-in.
* Companion libraries such as [Stuart ML](https://github.com/BixData/stuart-ml) (a Lua port of Spark MLlib) will end up in their own separate Git repo and [LuaRocks module](http://luarocks.org/modules/drauschenbach/stuart-ml).

## Building

The LuaRocks built-in build system is used for packaging.

```bash
$ luarocks make rockspecs/stuart-<version>.rockspec
stuart <version> is now built and installed in /usr/local (license: Apache 2.0)
```

## Testing

Testing with `lua-cjson`:

```
$ luarocks install busted
$ luarocks install lua-cjson
$ busted -v --defer-print
17/11/12 08:46:51 INFO Running Stuart (Embedded Spark) version 2.2.0 
...
141 successes / 0 failures / 0 errors / 0 pending : 12.026833 seconds
```

Testing with `lunajson`:

```
$ luarocks remove lua-cjson
$ busted -v --defer-print
17/11/12 08:46:51 INFO Running Stuart (Embedded Spark) version 2.2.0 
...
139 successes / 0 failures / 0 errors / 2 pending : 12.026833 seconds

Pending → ...
util.json can decode a scalar using cjson
... cjson not installed

Pending → ...
util.json can decode an object using cjson
... cjson not installed
```

Testing with a WebHDFS endpoint:

```
$ WEBHDFS_URL=webhdfs://localhost:50075/webhdfs busted -v --defer-print
```
