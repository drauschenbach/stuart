local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('RDD', function()

  local sc = stuart.NewContext()
  
  it('combineByKey() example at http://codingjunkie.net/spark-combine-by-key/ works', function()
    local initialScores = {
      {'Fred' , 88.0},
      {'Fred' , 95.0},
      {'Fred' , 91.0},
      {'Wilma', 93.0},
      {'Wilma', 95.0},
      {'Wilma', 98.0}
    }
    local wilmaAndFredScores = sc:parallelize(initialScores, 3):cache()
    
    local createScoreCombiner = function(score)
      return {1, score}
    end
    
    local scoreCombiner = function(collector, score)
      local numberScores, totalScore = collector[1], collector[2]
      return {numberScores + 1, totalScore + score}
    end
    
    local scoreMerger = function(collector1, collector2)
      local numScores1, totalScore1 = collector1[1], collector1[2]
      local numScores2, totalScore2 = collector2[1], collector2[2]
      return {numScores1 + numScores2, totalScore1 + totalScore2}
    end
    
    local scores = wilmaAndFredScores:combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)
    
    local averagingFunction = function(personScore)
      local name, numberScores, totalScore = personScore[1], personScore[2][1], personScore[2][2]
      return {name, totalScore / numberScores}
    end
    
    local averageScores = {}
    for key, value in pairs(scores:collectAsMap()) do
      averageScores[#averageScores+1] = averagingFunction({key, value})
    end
    
    for _, ps in ipairs(averageScores) do
      local name, average = ps[1], ps[2]
      --print(name .. '\'s average score : ' .. average)
      if name == 'Fred' then
        assert.equal_absTol(average, 91.33333, 0.00001)
      else
        assert.equal_absTol(average, 95.33333, 0.00001)
      end
    end
  end)

end)
