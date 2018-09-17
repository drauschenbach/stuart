local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD stdev()', function()

  local sc = stuart.NewContext()

  it('stdev()', function()
    local d = sc:parallelize({0.0, 0.0, 0.0}, 3)
    local res10 = d:popStdev()
    assert.equals(0.0, res10)

    d = sc:parallelize({0.0, 1.0}, 3)
    local res18 = d:popStdev()
    local res19 = d:sampleStdev()
    local res20 = d:stdev()
    assert.equals(0.5, res18)
    assert.equals(res18, res20)
    assert.is_in_range(res19, 0.7071067811864, 0.7071067811866)

    d = sc:parallelize({0.0, 0.0, 1.0}, 3)
    local res14 = d:popStdev()
    assert.equals(0.4714045207910317, res14)

    d = sc:parallelize({1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0}, 2)
    local res15 = d:popStdev()
    assert.equals(8.1268594453481508566, res15)

    d = sc:parallelize({9, 2, 5, 4, 12, 7}, 3)
    res10 = d:sampleStdev()
    assert.equals(3.6193922141707712825, res10)
  end)

end)
