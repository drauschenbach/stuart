local M = {}

local binomialMinSamplingRate = 1e-10

local binomialGetUpperBound = function(delta, n, fraction)
  local gamma = - math.log(delta) / n
  return math.min(1,
    math.max(binomialMinSamplingRate, fraction + gamma + math.sqrt(gamma * gamma + 2 * gamma * fraction)))
end

local poissonNumStd = function(s)
  if s < 6.0 then return 12.0 end
  if s < 16.0 then return 9.0 end
  return 6.0
end

local poissonGetUpperBound = function(s)
  return math.max(s + poissonNumStd(s) * math.sqrt(s), 1e-10)
end

M.computeFractionForSampleSize = function(sampleSizeLowerBound, total, withReplacement)
  if withReplacement then
    return poissonGetUpperBound(sampleSizeLowerBound) / total
  else
    local fraction = sampleSizeLowerBound / total
    return binomialGetUpperBound(1e-4, total, fraction)
  end
end

return M
