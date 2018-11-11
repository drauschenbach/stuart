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
    Zebra.__index = function(_, i)
      return i+10
    end
    local zebra = Zebra.new()
    assert.equals(15, zebra[5])
  end)
  
  it('subclassing works', function()
    local Animal = class.new()
    function Animal:name() return 'generic animal' end
    function Animal:_init(age) self.age = age end
    local animal = Animal.new(7)
    assert.equals('generic animal', animal:name())
    
    local Fish = class.new(Animal)
    function Fish:_init(age) self:super(age+1) end
    function Fish:name() return 'I am a fish' end
    assert.is_not_nil(Fish)
    local fish = Fish.new(9)
    assert.equals('I am a fish', fish:name())
  end)
  
end)
