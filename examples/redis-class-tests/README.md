# Class framework unit testing with Redis

The [stuart-elua](https://github.com/BixData/stuart-elua) project provides self-contained unit tests such as [test/stuart_class.lua](https://github.com/BixData/stuart-elua/blob/2.0.0-0/test/stuart_class.lua) which are well-suited to being sent into Redis to report on test coverage.

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

### Step 3: Amalgamate the unit test suite with its dependencies

```sh
$ amalg-redis.lua -s stuart_class-tests.lua -o stuart_class-tests-with-dependencies.lua -c
```

## Invoking

Tail the Redis log in one shell session:

```sh
$ tail -f /usr/local/var/log/redis.log
```

Then submit the test suite in another:

```sh
$ redis-cli --eval stuart_class-tests-with-dependencies.lua 0,0
```
