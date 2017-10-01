local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('distinct() works on pairs', function()
    local rdd = sc:parallelize({{0,1}, {0,1}})
    local actual = rdd:distinct():collect()
    assert.same({{0,1}}, actual)
  end)

end)
