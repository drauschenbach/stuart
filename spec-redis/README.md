# Unit testing within Redis

The [stuart-elua](https://github.com/BixData/stuart-elua) project provides self-contained test suites such as [test/stuart_class.lua](https://github.com/BixData/stuart-elua/blob/2.0.0-0/test/stuart_class.lua) which are well-suited to being sent into Redis to report on test coverage.

A successful test run shows up in the Redis log like this:

```
22385:M 23 Feb 2019 07:00:31.248 - Accepted 127.0.0.1:63727
Begin test
✓ class creation works
✓ can create a new instance
✓ constructor params work
✓ class functions are accessible to instance
✓ class __index metamethod works
✓ subclassing works
✓ subclass __index metamethod works
End of test: 0 failures
22385:M 23 Feb 2019 07:00:31.250 - Client closed connection
```

## Configure Redis to show debug logging

Lua print statements will show up in the Redis log by setting `loglevel debug` within `redis.conf`.

On a Mac using the Homebrew distribution of Redis:

```sh
$ sudo vi /usr/local/etc/redis.conf
loglevel debug
$ brew services restart redis
$ tail -f /usr/local/var/log/redis.log
```

## Developing

### Step 1: Download the Lua Amalgamator for Redis

```sh
$ luarocks install amalg-redis
```

### Step 2: Generate an `amalg.cache` file

Using your local OS and its Lua VM, perform a trial run of the class framework test suite, while allowing `amalg-redis` to capture the module dependencies that are used during execution.

```sh
$ lua -lamalg-redis stuart_class-tests.lua
Begin test
✓ class creation works
✓ can create a new instance
✓ constructor params work
✓ class functions are accessible to instance
✓ class __index metamethod works
✓ subclassing works
✓ subclass __index metamethod works
End of test: 0 failures
```

This produces an `amalg.cache` file in the current directory, which is used by the amalgamation process.

### Step 3: Amalgamate the test suite with its dependencies

```sh
$ amalg-redis.lua -s stuart_class.lua -o stuart_class-with-dependencies.lua -c -i "^socket"
```

## Running a test suite

Tail the Redis log in one shell session:

```sh
$ tail -f /usr/local/var/log/redis.log
```

Then submit the test suite in another:

```sh
$ redis-cli --eval stuart_class-with-dependencies.lua 0,0
```

## Suite: stuart

	$ redis-cli --eval stuart-with-dependencies.lua 0,0

```
Begin test
INFO Running Stuart (Embedded Spark 2.2.0)
✓ NewContext()
INFO Running Stuart (Embedded Spark 2.2.0)
✓ NewStreamingContext()
End of test: 0 failures
```

## Suite: stuart_class

	$ redis-cli --eval stuart_class-with-dependencies.lua 0,0

```
Begin test
✓ class creation works
✓ can create a new instance
✓ constructor params work
✓ class functions are accessible to instance
✓ class __index metamethod works
✓ subclassing works
✓ subclass __index metamethod works
End of test: 0 failures
```

## Suite: stuart_Partition

	$ redis-cli --eval stuart_Partition-with-dependencies.lua 0,0

```
Begin test
✓ new()
✓ _count()
✓ _flatten()
✓ _toLocalIterator()
End of test: 0 failures
```

## Suite: stuart_RDD

	$ redis-cli --eval stuart_RDD-with-dependencies.lua 0,0

```
Begin test
INFO Running Stuart (Embedded Spark 2.2.0)
✓ assertContains() works
✓ assertContainsKeyedPair() works
✓ aggregate() Examples 1
✓ aggregate() Examples 2
✓ aggregateByKey()
✓ cartesian()
✓ coalesce()
✓ context, sparkContext
✓ count()
✓ countByKey()
✓ countByValue()
✓ distinct()
✓ filter() without mixed data
✓ filter() with mixed data
✓ first()
✓ flatMap()
✓ flatMapValues()
✓ fold()
✓ foldByKey()
✓ foreach()
✓ foreachPartition()
✓ glom()
✓ groupBy()
✓ groupByKey()
✓ histogram() with even spacing
✓ histogram() with custom spacing
✓ id
✓ intersection()
✓ isEmpty()
✓ join()
✓ keyBy()
✓ keys()
✓ leftOuterJoin()
✓ lookup()
✓ mapPartitions() Example 1
✓ mapPartitionsWithIndex()
✓ mapValues()
✓ max()
✓ mean()
✓ min()
✓ reduce()
✓ reduceByKey()
✓ repartition()
✓ name, setName()
✓ partitions
✓ sortBy()
✓ stats()
✓ stdev()
✓ subtract()
✓ subtractByKey()
✓ take()
✓ toLocalIterator()
✓ top()
✓ toString()
✓ treeAggregate()
✓ treeReduce()
✓ union()
✓ values()
✓ variance()
✓ zip()
✓ zipWithIndex()
```

## Suite: stuart_SparkConf

	$ redis-cli --eval stuart_SparkConf-with-dependencies.lua 0,0

```
Begin test
✓ works
End of test: 0 failures
```

## Suite: stuart_util

	$ redis-cli --eval stuart_util-with-dependencies.lua 0,0

```
Begin test
✓ jsonDecode() can decode a scalar
✓ jsonDecode() can decode an object
✓ split()
End of test: 0 failures
```

## Suite: moses

Stuart makes heavy use of [Moses](https://github.com/Yonaba/Moses), so it's also useful to see how much of Moses works within Redis.

	$ redis-cli --eval moses-with-dependencies.lua 0,0

```
Begin test
✓ assertContains() works
✓ operator.add()
✓ operator.concat()
✓ operator.div()
✓ operator.eq()
✓ operator.exp()
✓ operator.floordiv()
✓ operator.ge()
✓ operator.gt()
✓ operator.intdiv()
✓ operator.land()
✓ operator.le()
✓ operator.length()
✓ operator.lnot()
✓ operator.lor()
✓ operator.lt()
✓ operator.mod()
✓ operator.mul()
✓ operator.neq()
✓ operator.sub()
✓ operator.unm()
✓ all(t, f)
✓ append()
✓ clone({})
✓ clone(obj)
✖ count(t, val)
  FAILED: user_script:4406: Expected 2 but got 3
✓ countf(t, f)
✓ detect(t, value)
✓ difference(array1, array2)
✓ each(t,f)
✓ fill() replacing all
✓ fill() starting at index
✓ fill() replace within range
✓ fill() can enlarge the array
✓ find(array, value)
✓ find(array, value, from)
✓ first(array, n)
✓ flatten()
✓ identity()
✓ include()
✓ indexOf()
✓ intersection()
✓ isBoolean()
✓ isCallable()
✓ isEmpty()
✓ isEqual()
✓ isFinite()
✓ isFunction()
✓ isInteger()
✓ isNil()
✓ isNumber()
✓ isString()
✓ isTable()
✓ keys(obj)
✓ map(array)
✓ map(table)
✓ max(t)
✓ max(t, transform)
✓ min(t)
✓ min(t, transform)
✓ noop()
✓ ones(n)
✓ pack(...)
✓ partition(array, n=2)
✓ partition(array, n=4)
✓ pluck)
✓ range(from)
✓ range(from, to)
✓ range(from, to, step)
✓ reduce(t, f)
✓ reduce(t, f, state)
✓ rep()
✓ result()
✓ reverse()
✓ same()
✓ select()
✓ size()
✓ slice()
✓ sort()
✓ sort() with a custom comparison function
✓ sortBy() with transform function
✓ sortBy() with name property
✓ sortBy() with name property and custom comparator
✓ sortBy() defaults to identity
✓ tabulate()
✓ unique()
✓ values(obj)
✓ zeros(n)
✓ zip(...)
End of test: 2 failures
```

## Related

* [moses-elua](https://github.com/BixData/moses-elua)
* [stuart-elua](https://github.com/BixData/stuart-elua)
* [stuart-ml](https://github.com/BixData/stuart-ml)
* [stuart-ml-elua](https://github.com/BixData/stuart-ml-elua)
