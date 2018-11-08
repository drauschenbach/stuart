local registerAsserts = require 'registerAsserts'
local SparkConf = require 'stuart.SparkConf'
local SparkContext = require 'stuart.Context'

registerAsserts(assert)

describe('Apache Spark 2.2.0 SparkConfSuite', function()

--  it('Test byteString conversion', function()
--    val conf = new SparkConf()
--    -- Simply exercise the API, we don't need a complete conversion test since that's handled in
--    -- UtilsSuite.scala
--    assert(conf.getSizeAsBytes('fake', '1k') === ByteUnit.KiB.toBytes(1))
--    assert(conf.getSizeAsKb('fake', '1k') === ByteUnit.KiB.toKiB(1))
--    assert(conf.getSizeAsMb('fake', '1k') === ByteUnit.KiB.toMiB(1))
--    assert(conf.getSizeAsGb('fake', '1k') === ByteUnit.KiB.toGiB(1))
--  }
--
--  it('Test timeString conversion', function()
--    val conf = new SparkConf()
--    -- Simply exercise the API, we don't need a complete conversion test since that's handled in
--    -- UtilsSuite.scala
--    assert(conf.getTimeAsMs('fake', '1ms') === TimeUnit.MILLISECONDS.toMillis(1))
--    assert(conf.getTimeAsSeconds('fake', '1000ms') === TimeUnit.MILLISECONDS.toSeconds(1000))
--  }
--
--  it('loading from system properties', function()
--    System.setProperty('spark.test.testProperty', '2')
--    System.setProperty('nonspark.test.testProperty', '0')
--    val conf = new SparkConf()
--    assert(conf.get('spark.test.testProperty') === '2')
--    assert(!conf.contains('nonspark.test.testProperty'))
--  }
--
--  it('initializing without loading defaults', function()
--    System.setProperty('spark.test.testProperty', '2')
--    val conf = new SparkConf(false)
--    assert(!conf.contains('spark.test.testProperty'))
--  }

  it('named set methods', function()
    local conf = SparkConf:new(false)

    conf:setMaster('local[3]')
    conf:setAppName('My app')
    conf:setSparkHome('/path')
    --conf.setJars(Seq('a.jar', 'b.jar'))
    --conf.setExecutorEnv('VAR1', 'value1')
    --conf.setExecutorEnv(Seq(('VAR2', 'value2'), ('VAR3', 'value3')))

    assert.equal('local[3]', conf:get('spark.master'))
    assert.equal('My app', conf:get('spark.app.name'))
    assert.equal('/path', conf:get('spark.home'))
    --assert.equal('a.jar,b.jar', conf:get('spark.jars'))
    --assert.equal('value1', conf:get('spark.executorEnv.VAR1'))
    --assert.equal('value2', conf:get('spark.executorEnv.VAR2'))
    --assert.equal('value3', conf:get('spark.executorEnv.VAR3'))
  end)

  it('basic get and set', function()
    local conf = SparkConf:new(false)
    assert.same({}, conf:getAll())
    conf:set('k1', 'v1')
    conf:setAll({{'k2','v2'}, {'k3','v3'}})
    do
      local all = conf:getAll()
      assert.contains_pair(all, {'k1','v1'})
      assert.contains_pair(all, {'k2','v2'})
      assert.contains_pair(all, {'k3','v3'})
    end
    conf:set('k1', 'v4')
    conf:setAll({{'k2','v5'}, {'k3','v6'}})
    do
      local all = conf:getAll()
      assert.contains_pair(all, {'k1','v4'})
      assert.contains_pair(all, {'k2','v5'})
      assert.contains_pair(all, {'k3','v6'})
    end
    assert.is_true(conf:contains('k1'))
    assert.is_not_true(conf:contains('k4'))
    assert.equal('v4', conf:get('k1'))
    --intercept[Exception] { conf.get('k4') }
    assert.equal('not found', conf:get('k4', 'not found'))
    assert.equal('v4', conf:getOption('k1'))
    assert.equal(nil, conf:getOption('k4'))
  end)

--  it('creating SparkContext without master and app name', function()
--    local conf = SparkConf:new(false)
--    intercept[SparkException] { sc = new SparkContext(conf) }
--  end)

--  it('creating SparkContext without master', function()
--    val conf = new SparkConf(false).setAppName('My app')
--    intercept[SparkException] { sc = new SparkContext(conf) }
--  }
--
--  it('creating SparkContext without app name', function()
--    val conf = new SparkConf(false).setMaster('local')
--    intercept[SparkException] { sc = new SparkContext(conf) }
--  }

  it('creating SparkContext with both master and app name', function()
    local conf = SparkConf:new(false):setMaster('local'):setAppName('My app')
    local sc = SparkContext:new(conf)
    assert.equal('local', sc:master())
    assert('My app', sc:appName())
  end)

--  it('SparkContext property overriding', function()
--    val conf = new SparkConf(false).setMaster('local').setAppName('My app')
--    sc = new SparkContext('local[2]', 'My other app', conf)
--    assert(sc.master === 'local[2]')
--    assert(sc.appName === 'My other app')
--  }
--
--  it('nested property names', function()
--    -- This wasn't supported by some external conf parsing libraries
--    System.setProperty('spark.test.a', 'a')
--    System.setProperty('spark.test.a.b', 'a.b')
--    System.setProperty('spark.test.a.b.c', 'a.b.c')
--    val conf = new SparkConf()
--    assert(conf.get('spark.test.a') === 'a')
--    assert(conf.get('spark.test.a.b') === 'a.b')
--    assert(conf.get('spark.test.a.b.c') === 'a.b.c')
--    conf.set('spark.test.a.b', 'A.B')
--    assert(conf.get('spark.test.a') === 'a')
--    assert(conf.get('spark.test.a.b') === 'A.B')
--    assert(conf.get('spark.test.a.b.c') === 'a.b.c')
--  }
--
--  it('Thread safeness - SPARK-5425', function()
--    val executor = Executors.newSingleThreadScheduledExecutor()
--    val sf = executor.scheduleAtFixedRate(new Runnable {
--      override def run(): Unit =
--        System.setProperty('spark.5425.' + Random.nextInt(), Random.nextInt().toString)
--    }, 0, 1, TimeUnit.MILLISECONDS)
--
--    try {
--      val t0 = System.currentTimeMillis()
--      while ((System.currentTimeMillis() - t0) < 1000) {
--        val conf = Try(new SparkConf(loadDefaults = true))
--        assert(conf.isSuccess === true)
--      }
--    } finally {
--      executor.shutdownNow()
--      val sysProps = System.getProperties
--      for (key <- sysProps.stringPropertyNames().asScala if key.startsWith('spark.5425.'))
--        sysProps.remove(key)
--    }
--  }
--
--  it('register kryo classes through registerKryoClasses', function()
--    val conf = new SparkConf().set('spark.kryo.registrationRequired', 'true')
--
--    conf.registerKryoClasses(Array(classOf[Class1], classOf[Class2]))
--    assert(conf.get('spark.kryo.classesToRegister') ===
--      classOf[Class1].getName + ',' + classOf[Class2].getName)
--
--    conf.registerKryoClasses(Array(classOf[Class3]))
--    assert(conf.get('spark.kryo.classesToRegister') ===
--      classOf[Class1].getName + ',' + classOf[Class2].getName + ',' + classOf[Class3].getName)
--
--    conf.registerKryoClasses(Array(classOf[Class2]))
--    assert(conf.get('spark.kryo.classesToRegister') ===
--      classOf[Class1].getName + ',' + classOf[Class2].getName + ',' + classOf[Class3].getName)
--
--    -- Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
--    -- blow up.
--    val serializer = new KryoSerializer(conf)
--    serializer.newInstance().serialize(new Class1())
--    serializer.newInstance().serialize(new Class2())
--    serializer.newInstance().serialize(new Class3())
--  }
--
--  it('register kryo classes through registerKryoClasses and custom registrator', function()
--    val conf = new SparkConf().set('spark.kryo.registrationRequired', 'true')
--
--    conf.registerKryoClasses(Array(classOf[Class1]))
--    assert(conf.get('spark.kryo.classesToRegister') === classOf[Class1].getName)
--
--    conf.set('spark.kryo.registrator', classOf[CustomRegistrator].getName)
--
--    -- Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
--    -- blow up.
--    val serializer = new KryoSerializer(conf)
--    serializer.newInstance().serialize(new Class1())
--    serializer.newInstance().serialize(new Class2())
--  }
--
--  it('register kryo classes through conf', function()
--    val conf = new SparkConf().set('spark.kryo.registrationRequired', 'true')
--    conf.set('spark.kryo.classesToRegister', 'java.lang.StringBuffer')
--    conf.set('spark.serializer', classOf[KryoSerializer].getName)
--
--    -- Kryo doesn't expose a way to discover registered classes, but at least make sure this doesn't
--    -- blow up.
--    val serializer = new KryoSerializer(conf)
--    serializer.newInstance().serialize(new StringBuffer())
--  }
--
--  it('deprecated configs', function()
--    val conf = new SparkConf()
--    val newName = 'spark.history.fs.update.interval'
--
--    assert(!conf.contains(newName))
--
--    conf.set('spark.history.updateInterval', '1')
--    assert(conf.get(newName) === '1')
--
--    conf.set('spark.history.fs.updateInterval', '2')
--    assert(conf.get(newName) === '2')
--
--    conf.set('spark.history.fs.update.interval.seconds', '3')
--    assert(conf.get(newName) === '3')
--
--    conf.set(newName, '4')
--    assert(conf.get(newName) === '4')
--
--    val count = conf.getAll.count { case (k, v) => k.startsWith('spark.history.') }
--    assert(count === 4)
--
--    conf.set('spark.yarn.applicationMaster.waitTries', '42')
--    assert(conf.getTimeAsSeconds('spark.yarn.am.waitTime') === 420)
--
--    conf.set('spark.kryoserializer.buffer.mb', '1.1')
--    assert(conf.getSizeAsKb('spark.kryoserializer.buffer') === 1100)
--  }
--
--  it('akka deprecated configs', function()
--    val conf = new SparkConf()
--
--    assert(!conf.contains('spark.rpc.numRetries'))
--    assert(!conf.contains('spark.rpc.retry.wait'))
--    assert(!conf.contains('spark.rpc.askTimeout'))
--    assert(!conf.contains('spark.rpc.lookupTimeout'))
--
--    conf.set('spark.akka.num.retries', '1')
--    assert(RpcUtils.numRetries(conf) === 1)
--
--    conf.set('spark.akka.retry.wait', '2')
--    assert(RpcUtils.retryWaitMs(conf) === 2L)
--
--    conf.set('spark.akka.askTimeout', '3')
--    assert(RpcUtils.askRpcTimeout(conf).duration === (3 seconds))
--
--    conf.set('spark.akka.lookupTimeout', '4')
--    assert(RpcUtils.lookupRpcTimeout(conf).duration === (4 seconds))
--  }
--
--  it('SPARK-13727', function()
--    val conf = new SparkConf()
--    -- set the conf in the deprecated way
--    conf.set('spark.io.compression.lz4.block.size', '12345')
--    -- get the conf in the recommended way
--    assert(conf.get('spark.io.compression.lz4.blockSize') === '12345')
--    -- we can still get the conf in the deprecated way
--    assert(conf.get('spark.io.compression.lz4.block.size') === '12345')
--    -- the contains() also works as expected
--    assert(conf.contains('spark.io.compression.lz4.block.size'))
--    assert(conf.contains('spark.io.compression.lz4.blockSize'))
--    assert(conf.contains('spark.io.unknown') === false)
--  }
--
--  val serializers = Map(
--    'java' -> new JavaSerializer(new SparkConf()),
--    'kryo' -> new KryoSerializer(new SparkConf()))
--
--  serializers.foreach { case (name, ser) =>
--    test(s'SPARK-17240: SparkConf should be serializable ($name)', function()
--      val conf = new SparkConf()
--      conf.set(DRIVER_CLASS_PATH, '${' + DRIVER_JAVA_OPTIONS.key + '}')
--      conf.set(DRIVER_JAVA_OPTIONS, 'test')
--
--      val serializer = ser.newInstance()
--      val bytes = serializer.serialize(conf)
--      val deser = serializer.deserialize[SparkConf](bytes)
--
--      assert(conf.get(DRIVER_CLASS_PATH) === deser.get(DRIVER_CLASS_PATH))
--    }
--  }
--
--  it('encryption requires authentication', function()
--    val conf = new SparkConf()
--    conf.validateSettings()
--
--    conf.set(NETWORK_ENCRYPTION_ENABLED, true)
--    intercept[IllegalArgumentException] {
--      conf.validateSettings()
--    }
--
--    conf.set(NETWORK_ENCRYPTION_ENABLED, false)
--    conf.set(SASL_ENCRYPTION_ENABLED, true)
--    intercept[IllegalArgumentException] {
--      conf.validateSettings()
--    }
--
--    conf.set(NETWORK_AUTH_ENABLED, true)
--    conf.validateSettings()
--  }

end)
