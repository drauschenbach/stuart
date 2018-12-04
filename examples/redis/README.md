# Spark Pi in Redis!

To run the Spark Pi example app, the "Hello World" of Spark, in Redis:

```sh
$ redis-cli --eval SparkPi-with-dependencies.lua 0,0
"Pi is roughly 3.1332956664783"
```

## Developing

The amalgamated SparkPi-with-dependencies.lua was produced as follows:

### Step 1: Download Stuart

Start with an amalgamated distribution of Stuart on npmjs.com, accessible via the jsDelivr CDN:

```sh
$ wget https://cdn.jsdelivr.net/npm/lua-stuart@1.0.1-0/stuart.lua
```

### Step 2: Amalgamate Stuart with your Spark job

Concatenate the Spark job we want to run:

```sh
$ cat redis-support.lua stuart.lua SparkPi.lua > SparkPi-with-dependencies.lua
```

### Step 3: Manual Edits

In the last line of the script, change the print statement into a return value.

Instead of:

```lua
print('Pi is roughly ' .. 4 * count / (n-1))
```

Use:

```lua
return 'Pi is roughly ' .. 4 * count / (n-1)
```

Remove all occurances of `local _ENV = _ENV` (something injected by [lua-amalg](https://github.com/siffiejoe/lua-amalg) that turns out to be operating system specific).

Remove all occurances of `local arg = _G.arg;` (something injected by [lua-amalg](https://github.com/siffiejoe/lua-amalg) that turns out to be operating system specific).

Search for every instance of `io.`, which is a use of the io module (mostly by the Moses library), and put the following line of code before it so that it resolves when Redis is compiling the script: `local io = {}`

## Going Further

Also amalgamate [Stuart ML](https://www.jsdelivr.com/package/npm/lua-stuart-ml) to make use of statistics, Vectors, and K-means capabilities within Redis.
