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

  it('can textFile() a file', function()
    local rdd = sc:textFile('spec-fixtures/aristotle2.txt')
    local lines = rdd:collect()
    assert.equal(2, #lines)
    assert.equal('We are what we repeatedly do. Excellence, then, is not an act, but a habit.', lines[1])
    assert.equal('It is the mark of an educated mind to be able to entertain a thought without accepting it.', lines[2])
  end)
  
  it('can textFile() a directory', function()
    local rdd = sc:textFile('spec-fixtures/')
    local lines = rdd:collect()
    assert.equal(3, #lines)
  end)
  
  it('can textFile() a WebHDFS directory', function()
    local webhdfsUrl = os.getenv('WEBHDFS_URL')
    if not webhdfsUrl then return pending('No WEBHDFS_URL is configured') end
    local rdd = sc:textFile('webhdfs://localhost:17072/model2/metadata')
    local lines = rdd:collect()
    assert.equal(1, #lines)
  end)
  
end)
