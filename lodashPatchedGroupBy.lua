local _ = require 'lodash'
local callIteratee = require 'lodashCallIteratee' 

-- This patched version of groupBy doesn't munge key types into strings
-- by unwanted use of _.str()
function groupBy(collection, iteratee)
  local t = {}
  for k, v in _.iter(collection) do
    local r = callIteratee(iteratee, selfArg, v, k, collection)
    if _.isNil(t[r]) then
      t[r] = {v}
    else
      _.push(t[r], v)
    end
  end
  return t
end

return groupBy
