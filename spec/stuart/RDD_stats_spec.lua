local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD stats()', function()

  local sc = stuart.NewContext()

  it('stats()', function()
    local x = sc:parallelize({1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0}, 2)
    local stats = x:stats()
    assert.equals(9, stats.count)
    assert.is_in_range(stats.mean, 11.26, 11.27)
    assert.is_in_range(stats.stdev, 8.619836135, 8.619836137)
    assert.is_in_range(stats.popStdev, 8.126859445, 8.126859447)
    assert.is_in_range(stats.variance, 74.30157500, 74.30157502)
    assert.is_in_range(stats.popVariance, 66.04584444, 66.04584446)

    -- empty should yield zeroes
    x = sc:parallelize({}, 2)
    stats = x:stats()
    assert.equals(0, stats.count)
    assert.equals(0, stats.stdev)
    assert.equals(0, stats.popStdev)
    assert.equals(0, stats.variance)
    assert.equals(0, stats.popVariance)

    -- only one item, should yield zeroes
    x = sc:parallelize({22.0}, 2)
    stats = x:stats()
    assert.equals(1, stats.count)
    assert.equals(0, stats.stdev)
    assert.equals(0, stats.popStdev)
    assert.equals(0, stats.variance)
    assert.equals(0, stats.popVariance)

    x = sc:parallelize({600, 470, 170, 430, 300}, 3)
    stats = x:stats()
    assert.equals(5, stats.count)
    assert.is_in_range(stats.stdev, 164, 165)
    assert.is_in_range(stats.popStdev, 147, 148)
    assert.equals(stats.variance, 27130)
    assert.equals(stats.popVariance, 21704)

    x = sc:parallelize({9, 2, 5, 4, 12, 7, 8, 11, 9, 3, 7, 4, 12, 5, 4, 10, 9, 6, 9, 4}, 2)
    stats = x:stats()
    assert.equals(20, stats.count)
    assert.is_in_range(stats.stdev, 3.060787652325, 3.060787652327)
    assert.is_in_range(stats.popStdev, 2.9832867780352, 2.9832867780354)
    assert.is_in_range(stats.variance, 9.3684210526315, 9.3684210526316)
    assert.equals(stats.popVariance, 8.9)
  end)

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
