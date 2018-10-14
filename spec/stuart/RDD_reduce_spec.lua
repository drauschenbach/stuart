local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('reduce() works with strings', function()
    local a = sc:parallelize({'a','b','c','d','e','f'}, 3)
    local actual = a:reduce(function(r,x) return r..x end)
    assert.equals('abcdef', actual)
  end)

end)
