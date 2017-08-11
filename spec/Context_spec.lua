local _ = require 'lodash'
local inspect = require 'inspect'
local stuart = require 'stuart'

describe('Context', function()

  local sc = stuart.NewContext()
  
  it('can parallelize() and return an RDD', function()
    local rdd = sc:parallelize({'Gnu', 'Cat'})
    assert.is_not_nil(rdd)
    assert.is_function(rdd['count'])
  end)

  it('can parallelize() pairs and return an RDD with PairRDDFunctions mixin', function()
    local rdd = sc:parallelize({{3,'Gnu'}, {3,'Yak'}})
    assert.is_not_nil(rdd)
    assert.is_function(rdd['count'])
    assert.is_function(rdd['countByKey'])
  end)

  it('can parallelize() into expected numbers of partitions', function()
    local rdd = sc:parallelize({'Gnu', 'Cat'})
    assert.equals(1, _.size(rdd:partitions()))
    
    rdd = sc:parallelize({'12','23','','345'}, 2)
    assert.equals(2, _.size(rdd:partitions()))
  end)

end)
