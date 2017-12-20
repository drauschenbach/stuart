---
-- lodash for lua
-- @author Daniel Moghimi (daniel.mogimi@gmail.com)
-- @license MIT
--
-- adapted to moses for the Stuart project
---

local moses = require 'stuart.util.moses'

local callIteratee = function (predicate, selfArg, ...)
  local result
  predicate = predicate or moses.identity
  if selfArg then
    result = predicate(selfArg, ...)
  else
    result = predicate(...)
  end
  return result
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
local lodashFind = function (collection, predicate, selfArg)
  for k, v in ipairs(collection) do
    if callIteratee(predicate, selfArg, v, k, collection) then
      return v
    end
  end
end

return lodashFind
