local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD sortBy()', function()

  local sc = stuart.NewContext()
  
  it('allows lexicographical sorting by return of a list (regression test issue-129)', function()
    local rdd = sc:parallelize({{'a',3}, {'c',1}, {'b',1}}, 2)
    local actual = rdd:sortBy(function(x) return {x[2], x[1]} end):collect()
    local expected = {{'b',1}, {'c',1}, {'a',3}}
    assert.same(expected, actual)
  end)

end)
