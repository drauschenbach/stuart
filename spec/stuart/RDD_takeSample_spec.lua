local class = require 'stuart.class'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('takeSample() works when RDDs contain Vector classes', function()
    local DenseVector = class.new('DenseVector')
    function DenseVector:__init(data)
      self.data = data
    end
    local vector1 = DenseVector.new({1,2,3})
    local vector2 = DenseVector.new({4,5,6})
    local rdd = sc:parallelize({vector1, vector2})
    local sample = rdd:takeSample(false, 1)
    assert.equal(1, #sample)
    assert.is_true(class.istype(sample[1], 'DenseVector'))
  end)

end)
