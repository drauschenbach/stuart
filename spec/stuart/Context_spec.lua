local _ = require 'lodash'
local stuart = require 'stuart'

describe('Context', function()

  local sc = stuart.NewContext()
  
  it('can parallelize()', function()
    local rdd = sc:parallelize({'Gnu', 'Cat'})
    assert.is_not_nil(rdd)
    assert.is_function(rdd['count'])
  end)

  it('can parallelize() pairs', function()
    local rdd = sc:parallelize({{3,'Gnu'}, {3,'Yak'}})
    assert.is_not_nil(rdd)
    assert.is_function(rdd['countByKey'])
  end)

  it('can parallelize() into expected numbers of partitions', function()
    local rdd = sc:parallelize({'Gnu', 'Cat'})
    assert.equals(1, #rdd.partitions)
    
    rdd = sc:parallelize({'12','23','','345'}, 2)
    assert.equals(2, #rdd.partitions)
    assert.equals(2, #rdd.partitions)
  end)

end)
