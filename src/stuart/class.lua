local classes = {}

local function _call(self, ...) return self:new(...) end

local M = {}

local DefaultMixin = {
  __tostring = function(self) return "instance of " .. tostring(self.class) end,

  __init = function() end,

  isInstanceOf = function(self, aClass)
    return type(aClass) == 'table'
       and type(self) == 'table'
       and (self.class == aClass
            or type(self.class) == 'table'
            and type(self.class.isSubclassOf) == 'function'
            and self.class:isSubclassOf(aClass))
  end,

  static = {
    allocate = function(self)
      assert(type(self) == 'table', "Make sure that you are using 'Class:allocate' instead of 'Class.allocate'")
      return setmetatable({class=self}, self.__instanceDict)
    end,

    new = function(self, ...)
      assert(type(self) == 'table', 'Make sure that you are using Class:new instead of Class.new')
      local instance = self:allocate()
      instance:__init(...)
      return instance
    end,

    subclass = function(self, name)
      assert(type(self) == 'table', string.format('Make sure that you are using %s:subclass instead of %s.subclass', name, name))
      assert(type(name) == "string", "You must provide a name(string) for your class")

      local subclass = M._createClass(name, self)

      for methodName, f in pairs(self.__instanceDict) do
        M._propagateInstanceMethod(subclass, methodName, f)
      end
      subclass.__init = function(instance, ...) return self.__init(instance, ...) end

      self.subclasses[subclass] = true
      self:subclassed(subclass)

      return subclass
    end,

    subclassed = function() end,

    isSubclassOf = function(self, other)
      return type(other) == 'table' and type(self.super) == 'table' and
             ( self.super == other or self.super:isSubclassOf(other) )
    end,

    include = function(self, ...)
      assert(type(self) == 'table', "Make sure you that you are using 'Class:include' instead of 'Class.include'")
      for _,mixin in ipairs({...}) do M._includeMixin(self, mixin) end
      return self
    end
  }
}

function M._createClass(name, super)
  local dict = {}
  dict.__index = dict

  local aClass = {
    __typename=name,
    super=super,
    static = {},
    __instanceDict = dict, __declaredMethods = {},
    subclasses = setmetatable({}, {__mode='k'})
  }

  if super then
    setmetatable(aClass.static, {
      __index = function(_,k)
        local result = rawget(dict,k)
        if result == nil then
          return super.static[k]
        end
        return result
      end
    })
  else
    setmetatable(aClass.static, { __index = function(_,k) return rawget(dict,k) end })
  end

  setmetatable(aClass, {
    __index = aClass.static,
    __call = _call,
    __newindex = M._declareInstanceMethod
  })

  return aClass
end

function M._createIndexWrapper(aClass, f)
  if f == nil then
    return aClass.__instanceDict
  else
    return function(self, name)
      local value = aClass.__instanceDict[name]
      if value ~= nil then
        return value
      elseif type(f) == "function" then
        return (f(self, name))
      else
        return f[name]
      end
    end
  end
end

function M._declareInstanceMethod(aClass, name, f)
  aClass.__declaredMethods[name] = f
  if f == nil and aClass.super then
    f = aClass.super.__instanceDict[name]
  end
  M._propagateInstanceMethod(aClass, name, f)
end

function M._includeMixin(aClass, mixin)
  local moses = require 'moses'
  assert(moses.isTable(mixin), "mixin must be a table")

  for name,method in pairs(mixin) do
    if name ~= "included" and name ~= "static" then aClass[name] = method end
  end

  for name,method in pairs(mixin.static or {}) do
    aClass.static[name] = method
  end

  if type(mixin.included)=="function" then mixin:included(aClass) end
  return aClass
end

function M._propagateInstanceMethod(aClass, name, f)
  f = name == "__index" and M._createIndexWrapper(aClass, f) or f
  aClass.__instanceDict[name] = f
  for subclass in pairs(aClass.subclasses) do
    if rawget(subclass.__declaredMethods, name) == nil then
      M._propagateInstanceMethod(subclass, name, f)
    end
  end
end

function M.istype(obj, typename)
  local moses = require 'moses'
  if moses.isTable(obj) and obj.isInstanceOf ~= nil then
    return obj:isInstanceOf(classes[typename])
  end
  return false
end

function M.new(typename, supername)
  assert(type(typename) == 'string', "A name (string) is needed for the new class")
  assert(classes[typename] == nil, string.format('The class <%s> is already registered', typename))
  local super, class
  if supername ~= nil then
    super = classes[supername]
    assert(super ~= nil, string.format('Parent class <%s> does not exist', supername))
    class = super:subclass(typename)
  else
    class = M._createClass(typename)
    M._includeMixin(class, DefaultMixin)
  end
  classes[typename] = class
  return class, super
end

function M.type(obj)
  local moses = require 'moses'
  local res
  if obj.class ~= nil then
    res = moses.result(obj.class, '__typename')
  end
  return res or type(obj)
end

return M
