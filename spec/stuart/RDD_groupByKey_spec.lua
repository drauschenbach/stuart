local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD groupByKey()', function()

  local sc = stuart.NewContext()
  
  it('allows specifying numPartitions', function()
    local rdd2 = sc:parallelize({{'a',1}, {'b',1}, {'c',1}}, 2)
    assert.equal(2, #rdd2.partitions)
    local rdd3 = rdd2:groupByKey(3)
    assert.equal(3, #rdd3.partitions)
  end)

end)
