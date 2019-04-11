local stuart = require 'stuart'

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('first() raises error when RDD is emtpy', function()
    local rdd = sc:parallelize({})
    assert.has_error(function() rdd:first() end)
  end)

end)
