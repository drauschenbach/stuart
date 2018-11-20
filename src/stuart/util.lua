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
  if string.find(str, sep) == nil then return {str} end
  local result = {}
  local pattern = '(.-)' .. sep .. '()'
  local nb = 0
  local lastPos
  for part, pos in string.gmatch(str, pattern) do
    nb = nb + 1
    result[nb] = part
    lastPos = pos
  end
  result[nb+1] = string.sub(str, lastPos)
  return result
end

--[[
Url parse which returns these string fields that conform to the optimized 'url' native module:
 - scheme   'http', 'webhdfs'
 - user     'joe' or nil
 - userinfo 'joe:password' or nil
 - host     '127.0.0.1:50070'
 - hostname '127.0.0.1'
 - port     '50070' or nil
 - path     '/a/b/foo.txt' or nil
 - query    '?op=OPEN' or nil
 - fragment 'chapter1' or nil
--]]
M.urlParse = function(s)
  local has_url, url = pcall(require, 'url')
  if has_url then
    return url.parse(s)
  end
  
  local netUrl = require 'net.url'
  local r = netUrl.parse(s)
  r.hostname = r.host
  if r.port then
    r.port = tostring(r.port)
    r.host = r.hostname .. ':' .. r.port
  end
  if r.query then
    r.query = '?' .. tostring(r.query)
  end
  r.authority = nil
  return r
end

return M
