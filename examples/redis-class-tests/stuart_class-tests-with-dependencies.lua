-- Begin Redis support
local package = {
  preload={}
}
local function require(name)
  return package.preload[name]()
end
local arg = ARGV
local io = nil
local os = nil
-- End Redis support


package.preload["stuart.class"] = function(...)
  -- adapted from https://github.com/stevedonovan/Microlight#classes (MIT License)
  -- external API adapted roughly to https://github.com/torch/class
  local M = {}
  function M.istype(obj, super)
    return super.classof(obj)
  end
  function M.new(base)
    local klass, base_ctor = {}
    if base then
      for k,v in pairs(base) do klass[k]=v end
      klass._base = base
      base_ctor = rawget(base,'_init') or function() end
    end
    klass.__index = klass
    klass._class = klass
    klass.classof = function(obj)
      local m = getmetatable(obj) -- an object created by class() ?
      if not m or not m._class then return false end
      while m do -- follow the inheritance chain
        if m == klass then return true end
        m = rawget(m,'_base')
      end
      return false
    end
    klass.new = function(...)
      local obj = setmetatable({},klass)
      if rawget(klass,'_init') then
        klass.super = base_ctor
        local res = klass._init(obj,...) -- call our constructor
        if res then -- which can return a new self..
          obj = setmetatable(res,klass)
        end
      elseif base_ctor then -- call base ctor automatically
          base_ctor(obj,...)
      end
      return obj
    end
    --setmetatable(klass, {__call=klass.new})
    return klass
  end
  return M
end


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


-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

local msg = string.format('End of test: %d failures', failures) 
print(msg)
return msg
