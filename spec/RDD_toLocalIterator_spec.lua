local _ = require 'lodash'
local stuart = require 'stuart'

assert:register('assertion', 'contains', function(state, arguments)
  local collection = arguments[1]
  local searchFor = arguments[2]
  return _.findIndex(collection, function(v) return v == searchFor end) > -1
end)

assert:register('assertion', 'not_contains', function(state, arguments)
  local collection = arguments[1]
  local searchFor = arguments[2]
  return _.findIndex(collection, function(v) return v == searchFor end) == -1
end)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('toLocalIterator()', function()
    local z = sc:parallelize({1,2,3,4,5,6}, 3)
    local iter = z:toLocalIterator()
    local actual = {}
    for e in iter do
      table.insert(actual, e)
    end
    assert.contains(actual, 1)
    assert.contains(actual, 2)
    assert.contains(actual, 3)
    assert.contains(actual, 4)
    assert.contains(actual, 5)
    assert.contains(actual, 6)
    assert.not_contains(actual, 0)
    assert.not_contains(actual, 7)
  end)

end)
