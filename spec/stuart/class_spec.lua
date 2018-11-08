local class = require 'stuart.class'

describe('class', function()

  it('registration works', function()
    class.new('Dog')
  end)
  
  it('duplicate registration fails', function()
    class.new('Bird')
    assert.has_error(function() class.new('Bird') end)
  end)
  
  it('can create a new instance', function()
    local Cat = class.new('Cat')
    local cat = Cat:new()
    assert.equals('Cat', class.type(cat))
  end)
  
  it('constructor params work', function()
    local Giraffe = class.new('Giraffe')
    function Giraffe:__init(age) self.age = age end
    local giraffe = Giraffe:new(7)
    assert.equals(7, giraffe.age)
  end)
  
  it('class functions are accessible to instance', function()
    local Frog = class.new('Frog')
    function Frog:isHungry() return true end
    local frog = Frog:new()
    assert.equals(true, frog:isHungry())
  end)
  
  it('class __index metamethod works', function()
    local Zebra = class.new('Zebra')
    Zebra.__index = function(_, i)
      return i+10
    end
    local zebra = Zebra:new()
    assert.equals(15, zebra[5])
  end)
  
  it('subclassing works', function()
    local Animal = class.new('Animal')
    function Animal:name() return 'generic animal' end
    local animal = Animal:new()
    assert.equals('generic animal', animal:name())
    
    local Fish, parent = class.new('Fish', 'Animal')
    assert.is_not_nil(Fish)
    assert.is_not_nil(parent)
    local fish = Fish:new()
    assert.equals('generic animal', fish:name())
    
    function Fish:name() return 'I am a fish' end
    fish = Fish:new()
    assert.equals('I am a fish', fish:name())
    assert.equals(true, class.istype(fish,'Animal'))
  end)
  
end)
