local lodashInRange = require 'stuart.util.lodashInRange'
local lodashStr = require 'stuart.util.lodashStr'
local moses = require 'moses'
local say = require 'say'

local registerAsserts = function(assert)

  -----------------------------------------------------------------------------
  say:set('assertion.contains.positive', 'Expected %s to contain %s')
  say:set('assertion.contains.negative', 'Expected %s to not contain %s')
  assert:register('assertion', 'contains', function(state, arguments)
    local collection = arguments[1]
    local searchFor = arguments[2]
    return moses.findIndex(collection, function(i,v) return v == searchFor end) ~= nil
  end, 'assertion.contains.positive', 'assertion.contains.negative')
  
  -----------------------------------------------------------------------------
  assert:register('assertion', 'contains_keyed_pair', function(state, arguments)
    local collection = arguments[1]
    local key = arguments[2]
    local value = arguments[3]
    for _, e in ipairs(collection) do
      if e[1] == key then
        if lodashStr(e[2]) == lodashStr(value) then return true end
      end
    end
    return false
  end)
  
  -----------------------------------------------------------------------------
  say:set('assertion.contains_pair.positive', 'Expected %s to contain pair %s')
  say:set('assertion.contains_pair.negative', 'Expected %s to not contain pair %s')
  assert:register('assertion', 'contains_pair', function(state, arguments)
    local collection = arguments[1]
    local value = arguments[2]
    for _, e in ipairs(collection) do
      if lodashStr(e) == lodashStr(value) then return true end
    end
    return false
  end, 'assertion.contains_pair.positive', 'assertion.contains_pair.negative')

  -----------------------------------------------------------------------------
  say:set('assertion.equal_absTol.positive', 'Expected %s to equal %s within absolute tolerance %s')
  say:set('assertion.equal_absTol.negative', 'Expected %s to not equal %s within absolute tolerance %s')
  assert:register('assertion', 'equal_absTol', function(_, arguments)
    local x = arguments[1]
    local y = arguments[2]
    local eps = arguments[3]
    if x == y then return true end
    return math.abs(x - y) < eps
  end, 'assertion.equal_absTol.positive', 'assertion.equal_absTol.negative')
  
  -----------------------------------------------------------------------------
  say:set('assertion.equal_relTol.positive', 'Expected %s to equal %s within relative tolerance %s')
  say:set('assertion.equal_relTol.negative', 'Expected %s to not equal %s within relative tolerance %s')
  assert:register('assertion', 'equal_relTol', function(_, arguments)
    local x = arguments[1]
    local y = arguments[2]
    local eps = arguments[3]
    if x == y then return true end
    local absX = math.abs(x)
    local absY = math.abs(y)
    local diff = math.abs(x - y)
    return diff < eps * math.min(absX, absY)
  end, 'assertion.equal_relTol.positive', 'assertion.equal_relTol.negative')
  
  -----------------------------------------------------------------------------
  say:set('assertion.is_in_range.positive', 'Expected %s to be between %s and %s')
  say:set('assertion.is_in_range.negative', 'Expected %s to not be between %s and %s')
  assert:register('assertion', 'is_in_range', function(state, arguments)
    local value = arguments[1]
    local min = arguments[2]
    local max = arguments[3]
    return lodashInRange(value, min, max)
  end, 'assertion.is_in_range.positive', 'assertion.is_in_range.negative')
  
end

return registerAsserts
