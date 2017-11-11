local moses = require 'moses'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

-- https://spark.apache.org/examples.html
describe('Apach Spark examples', function()

  local sc = stuart.NewContext()
  
  it('Pi Estimation', function()
    local NUM_SAMPLES = 5000
    local slices = 2
    local count = sc:parallelize(moses.range(1,NUM_SAMPLES), slices):filter(function()
      local x = math.random()
      local y = math.random()
      return x*x + y*y < 1
    end):count()
    local pi = 4 * count / NUM_SAMPLES
    assert.is_in_range(pi, 3, 3.5)
  end)
  
end)
