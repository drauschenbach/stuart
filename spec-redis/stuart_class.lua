-- Unit tests derived from https://github.com/Yonaba/Moses/blob/master/doc/tutorial.md
print('Begin test')

local class = require 'stuart.class'


-- ============================================================================
-- Mini test framework
-- ============================================================================

local failures = 0

local function assertEquals(expected,actual,message)
  message = message or string.format('Expected %s but got %s', tostring(expected), tostring(actual))
  assert(actual==expected, message)
end

local function it(message, testFn)
  local status, err =  pcall(testFn)
  if status then
    print(string.format('✓ %s', message))
  else
    print(string.format('✖ %s', message))
    print(string.format('  FAILED: %s', err))
    failures = failures + 1
  end
end


-- ============================================================================
-- stuart.class module
-- ============================================================================

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
  assertEquals(7, giraffe.age)
end)

it('class functions are accessible to instance', function()
  local Frog = class.new()
  function Frog:_init(age) self.age = age end
  function Frog:isHungry() return true end
  local frog = Frog.new(7)
  assertEquals(true, frog:isHungry())
end)

it('class __index metamethod works', function()
  local Zebra = class.new()
  Zebra.__index = function(self, key)
    if type(key)=='number' then return key+10 end
    return rawget(getmetatable(self), key)
  end
  function Zebra:age() return 7 end
  local zebra = Zebra.new()
  assertEquals(15, zebra[5])
  assertEquals(7, zebra:age())
end)

it('subclassing works', function()
  local Animal = class.new()
  function Animal:name() return 'generic animal' end
  function Animal:_init(age) self.age = age end
  local animal = Animal.new(7)
  assertEquals('generic animal', animal:name())
  
  local Fish = class.new(Animal)
  function Fish:_init(age) return Animal._init(self, age+1) end
  function Fish:name() return 'I am a fish' end
  local fish = Fish.new(9)
  assertEquals('I am a fish', fish:name())
  assertEquals(true, class.istype(fish, Fish))
  assertEquals(true, class.istype(fish, Animal))
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
  assertEquals(15, zebra[5])
  assertEquals(5, zebra:age())
  assertEquals(23, zebra.category())
  assertEquals(25, zebra.foo)
end)

it('Three levels of subclassing works', function()
  local Animal = class.new()
  local animal = Animal.new()
  
  local Zebra = class.new(Animal)
  local zebra = Zebra.new()
  assertEquals(true, class.istype(zebra, Animal))
  assertEquals(true, class.istype(zebra, Zebra))
  
  local SpottedZebra = class.new(Zebra)
  local spottedZebra = SpottedZebra.new()
  assertEquals(true, class.istype(spottedZebra, Animal))
  assertEquals(true, class.istype(spottedZebra, Zebra))
  assertEquals(true, class.istype(spottedZebra, SpottedZebra))
end)


-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

print(string.format('End of test: %d failures', failures))
