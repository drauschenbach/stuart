local class = require 'middleclass'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('takeSample() works when RDDs contain Vector classes', function()
    local DenseVector = class('DenseVector')
    function DenseVector:initialize(data)
      self.data = data
    end
    local vector1 = DenseVector:new({1,2,3})
    local vector2 = DenseVector:new({4,5,6})
    local rdd = sc:parallelize({vector1, vector2})
    local sample = rdd:takeSample(false, 1)
    assert.equal(1, #sample)
    assert.is_true(sample[1]:isInstanceOf(DenseVector))
  end)

end)
