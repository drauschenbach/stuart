# Spark Pi in Redis!

To run Spark Pi, Apache Spark's "Hello World" example app, in Redis:

```sh
$ redis-cli --eval SparkPi-with-dependencies.lua 0,0
"Pi is roughly 3.133295666"
```

## Developing

The amalgamated `SparkPi-with-dependencies.lua` was produced as follows:

### Step 1: Download Lua Amalgamator for Redis

```sh
$ luarocks install amalg-redis
```

### Step 2: Generate `amalg.cache` file

Using your local OS and its Lua VM, perform a trial run of your Spark job, while allowing `amalg-redis` to capture the module dependencies that are used during execution.

```sh
$ lua -lamalg-redis SparkPi.lua
INFO Running Stuart (Embedded Spark 2.2.0)
Pi is roughly 3.141515707
```

This produces an `amalg.cache` file in the current directory, which is required by the amalgamation process.

### Step 3: Preparation of Spark job for Redis

When Lua scripts run in Redis, print statements are not returned to `redis-cli`. So Spark jobs that produce output should do so with a `return` statement.

In the last line of the `SparkPi.lua` script, add a return value with the result you want the CLI caller to see. Leave in print statements as well so that you can still test and debug your script using the local operating system.

Instead of:

```lua
print('Pi is roughly ' .. 4 * count / (n-1))
```

Use:

```lua
local msg = 'Pi is roughly ' .. 4 * count / (n-1)
print(msg)
return msg
```

### Step 4: Amalgamate your Spark job with its dependencies

```sh
$ amalg-redis.lua -s SparkPi.lua -o SparkPi-with-dependencies.lua -c
```
