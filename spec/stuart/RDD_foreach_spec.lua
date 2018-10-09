local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('foreach() does not modify RDD (regression test issue-76)', function()
    local z = sc:parallelize({1,2,3})
    z:foreach(function(x) return x*2 end)
    assert.same({1,2,3}, z:collect())
  end)

end)
