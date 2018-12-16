local class = require 'stuart.class'

describe('class', function()

  it('class creation works', function()
    class.new()
  end)
  
  it('can create a new instance', function()
    local Cat = class.new()
    Cat.new()
  end)
  
  it('constructor params work', function()
    local Giraffe = class.new()
    function Giraffe:_init(age) self.age = age end
    local giraffe = Giraffe.new(7)
    assert.equals(7, giraffe.age)
  end)
  
  it('class functions are accessible to instance', function()
    local Frog = class.new()
    function Frog:_init(age) self.age = age end
    function Frog:isHungry() return true end
    local frog = Frog.new(7)
    assert.equals(true, frog:isHungry())
  end)
  
  it('class __index metamethod works', function()
    local Zebra = class.new()
    Zebra.__index = function(self, key)
      if type(key)=='number' then return key+10 end
      return rawget(getmetatable(self), key)
    end
    function Zebra:age() return 7 end
    local zebra = Zebra.new()
    assert.equals(15, zebra[5])
    assert.equals(7, zebra:age())
  end)
  
  it('subclassing works', function()
    local Animal = class.new()
    function Animal:name() return 'generic animal' end
    function Animal:_init(age) self.age = age end
    local animal = Animal.new(7)
    assert.equals('generic animal', animal:name())
    
    local Fish = class.new(Animal)
    function Fish:_init(age) return Animal._init(self, age+1) end
    function Fish:name() return 'I am a fish' end
    local fish = Fish.new(9)
    assert.equals('I am a fish', fish:name())
    assert.is_true(class.istype(fish, Fish))
    assert.is_true(class.istype(fish, Animal))
  end)
  
  it('subclass __index metamethod works', function()
    local Animal = class.new()
    function Animal:age() return 5 end
    function Animal.category() return 23 end
    Animal.foo = 25
    
    local Zebra = class.new(Animal)
    Zebra.__index = function(self, key)
      if type(key)=='number' then return key+10 end
      return rawget(getmetatable(self), key)
    end
    local zebra = Zebra.new()
    assert.equals(15, zebra[5])
    assert.equals(5, zebra:age())
    assert.equals(23, zebra.category())
    assert.equals(25, zebra.foo)
  end)
  
  it('Three levels of subclassing works (regression test #119)', function()
    local ssc = {sc={}}
    
    local DStream = require 'stuart.streaming.DStream'
    local dstream = DStream.new(ssc)
    assert.is_not_nil(dstream.inputs)
    
    local SocketInputDStream = require 'stuart.streaming.SocketInputDStream'
    local hostname = ''
    local port = 5000
    dstream = SocketInputDStream.new(ssc, hostname, port)

    local QueueInputDStream = require 'stuart.streaming.QueueInputDStream'
    dstream = QueueInputDStream.new(ssc, {}, true)
    assert.is_not_nil(dstream.inputs)
  end)
  
end)
