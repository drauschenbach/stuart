local M = {}

M.clone = function(zeroValue)
  local moses = require 'moses'
  if type(zeroValue) ~= 'table' then
    return moses.clone(zeroValue)
  end
  if type(zeroValue.clone) == 'function' then
    return zeroValue:clone()
  end
  if zeroValue.__typename ~= nil then
    error('Cannot clone a Stuart- or Torch-style class; you must provide it a clone() function')
  end
  if zeroValue.class ~= nil then
    error('Cannot clone a middleclass class; you must provide it a clone() function')
  end
  return moses.clone(zeroValue)
end

M.isInstanceOf = function(obj, klass)
  if type(obj) ~= 'table' then return false end
  if obj.isInstanceOf then return obj:isInstanceOf(klass) end -- middleclass
  if klass and klass.isInstanceOf then return false end -- middleclass ~= Torch class
  local class = require 'stuart.util.class'
  local objType = class.type(obj)
  local x = objType == klass or objType == class.type(klass) -- Torch class
  return x
end

M.jsonDecode = function(s)
  local has_cjson, cjson = pcall(require, 'cjson')
  if has_cjson then
    return cjson.decode(s)
  else
    local lunajsonDecoder = require 'lunajson.decoder'
    return lunajsonDecoder()(s)
  end
end

M.lodashCallIteratee = function (predicate, selfArg, ...)
  local moses = require 'moses'
  predicate = predicate or moses.identity
  if selfArg then
    return predicate(selfArg, ...)
  else
    return predicate(...)
  end
end

---
-- Iterates over elements of collection, returning the first element
-- predicate returns truthy for. The predicate is bound to selfArg and
-- invoked with three arguments: (value, index|key, collection).
-- @usage _.print(_.find({{a = 1}, {a = 2}, {a = 3}, {a = 2}, {a = 3}}, function(v)
--     return v.a == 3
-- end))
-- --> {[a]=3}
--
-- @param collection The collection to search. (table|string)
-- @param predicate The function invoked per iteration
-- @param selfArg The self binding of predicate.
M.lodashFind = function (collection, predicate, selfArg)
  for k, v in ipairs(collection) do
    if M.lodashCallIteratee(predicate, selfArg, v, k, collection) then
      return v
    end
  end
end

---
-- Checks if n is between start and up to but not including, end.
-- If end is not specified it’s set to start with start then set to 0.
-- @usage print(_.inRange(-3, -4, 8))
-- --> true
--
-- @param n The number to check.
-- @param start The start of the range.
-- @param stop The end of the range.
-- @return Returns true if n is in the range, else false.
M.lodashInRange = function (n, start, stop)
  local moses = require 'moses'
  local _start = moses.isNil(stop) and 0 or start or 0
  local _stop = moses.isNil(stop) and start or stop or 1
  return n >= _start and n < _stop
end

---
-- Cast anything to string. If any function detected, call and cast its
-- result.
--
-- @usage print(_.str({1, 2, 3, 4, {k=2, {'x', 'y'}}}))
-- --> {1, 2, 3, 4, {{"x", "y"}, ["k"]=2}}
-- print(_.str({1, 2, 3, 4, function(a) return a end}, 5))
-- --> {1, 2, 3, 4, 5}
--
-- lodash for lua
-- @author Daniel Moghimi (daniel.mogimi@gmail.com)
-- @license MIT
--
-- Adapted to Moses for the Stuart project.
--
-- @param value value to cast
-- @param ... The parameters to pass to any detected function
-- @return casted value
M.lodashStr = function (value, ...)
  local moses = require 'moses'
  local dblQuote = function (v)
    return '"'..v..'"'
  end
  local str = '';
  -- local v;
  if moses.isString(value) then
    str = value
  elseif moses.isBoolean(value) then
    str = value and 'true' or 'false'
  elseif moses.isNil(value) then
    str = 'nil'
  elseif moses.isNumber(value) then
    str = value .. ''
  elseif moses.isFunction(value) then
    str = M.str(value(...))
  elseif moses.isTable(value) then
    str = '{'
    for k, v in pairs(value) do
      v = moses.isString(v) and dblQuote(v) or M.lodashStr(v, ...)
      if moses.isNumber(k) then
        str = str .. v .. ', '
      else
        str = str .. '[' .. dblQuote(k) .. ']=' .. v .. ', '
      end
    end
    str = str:sub(0, #str - 2) .. '}'
  end
  return str
end


M.split = function(str, sep)
  local fields = {}
  local pattern = string.format('([^%s]+)', sep)
  str:gsub(pattern, function(c) fields[#fields+1] = c end)
  return fields
end

return M