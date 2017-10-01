local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('union() works on pairs', function()
    local a = sc:parallelize({{0,1}, {0,2}, {1,2}})
    local b = sc:parallelize({{0,2}, {2,1}, {2,2}})
    local actual = a:union(b):collect()
    assert.contains_pair(actual, {0,1})
    assert.contains_pair(actual, {0,2})
    assert.contains_pair(actual, {1,2})
    assert.contains_pair(actual, {2,1})
    assert.contains_pair(actual, {2,2})
  end)
  
  it('union() allows duplicate pairs', function()
    local a = sc:parallelize({{0,1}, {0,1}})
    local b = sc:parallelize({})
    local actual = a:union(b):collect()
    local expected = {{0,1}, {0,1}}
    assert.same(expected, actual)
  end)

end)
