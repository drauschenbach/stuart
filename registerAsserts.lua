local _ = require 'lodash'
local say = require 'say'

function registerAsserts(assert)

  say:set('assertion.contains.positive', 'Expected %s to contain %s')
  say:set('assertion.contains.negative', 'Expected %s to not contain %s')
  assert:register('assertion', 'contains', function(state, arguments)
    local collection = arguments[1]
    local searchFor = arguments[2]
    return _.findIndex(collection, function(v) return v == searchFor end) > -1
  end, 'assertion.contains.positive', 'assertion.contains.negative')
  
  say:set('assertion.is_in_range.positive', 'Expected %s to be between %s and %s')
  say:set('assertion.is_in_range.negative', 'Expected %s to not be between %s and %s')
  assert:register('assertion', 'is_in_range', function(state, arguments)
    local value = arguments[1]
    local min = arguments[2]
    local max = arguments[3]
    return _.inRange(value, min, max)
  end, 'assertion.is_in_range.positive', 'assertion.is_in_range.negative')
  
  assert:register('assertion', 'contains_keyed_pair', function(state, arguments)
    local collection = arguments[1]
    local key = arguments[2]
    local value = arguments[3]
    for i, e in ipairs(collection) do
      if e[1] == key then
        if _.str(e[2]) == _.str(value) then return true end
      end
    end
    return false
  end)
  
  say:set('assertion.contains_pair.positive', 'Expected %s to contain pair %s')
  say:set('assertion.contains_pair.negative', 'Expected %s to not contain pair %s')
  assert:register('assertion', 'contains_pair', function(state, arguments)
    local collection = arguments[1]
    local value = arguments[2]
    for i, e in ipairs(collection) do
      if _.str(e) == _.str(value) then return true end
    end
    return false
  end, 'assertion.contains_pair.positive', 'assertion.contains_pair.negative')

end

return registerAsserts
