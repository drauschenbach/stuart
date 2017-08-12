local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

-- https://spark.apache.org/examples.html
describe('Apach Spark examples', function()

  local sc = stuart.NewContext()
  
  it('Word count', function()
    local textFile = sc:textFile('spec-fixtures/aristotle1.txt')
    local counts = textFile:flatMap(function(line)
      local r = {}
      for w in string.gmatch(line, '%w+') do table.insert(r, w:lower()) end
      return r
    end):map(function(word)
      return {word, 1}
    end):reduceByKey(function(r, x)
      return r + x
    end):collect()
    assert.contains_pair(counts, {'in',2})
    assert.contains_pair(counts, {'job',1})
    assert.contains_pair(counts, {'perfection',1})
    assert.contains_pair(counts, {'pleasure',1})
    assert.contains_pair(counts, {'puts',1})
    assert.contains_pair(counts, {'the',2})
    assert.contains_pair(counts, {'work',1})
  end)
  
end)
