---
-- lodash for lua
-- @author Daniel Moghimi (daniel.mogimi@gmail.com)
-- @license MIT
--
-- adapted to moses for the Stuart project
---

local moses = require 'moses'

local dblQuote = function (v)
  return '"'..v..'"'
end

---
-- Cast anything to string. If any function detected, call and cast its
-- result.
-- @usage print(_.str({1, 2, 3, 4, {k=2, {'x', 'y'}}}))
-- --> {1, 2, 3, 4, {{"x", "y"}, ["k"]=2}}
-- print(_.str({1, 2, 3, 4, function(a) return a end}, 5))
-- --> {1, 2, 3, 4, 5}
--
-- @param value value to cast
-- @param ... The parameters to pass to any detected function
-- @return casted value
local M = {}
M.str = function (value, ...)
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
      v = moses.isString(v) and dblQuote(v) or M.str(v, ...)
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

return M.str
