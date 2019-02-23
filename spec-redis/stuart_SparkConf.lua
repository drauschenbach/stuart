print('Begin test')

local SparkConf = require 'stuart.SparkConf'


-- ============================================================================
-- Mini test framework
-- ============================================================================

local failures = 0

local function assertEqual(expected,actual,message)
  message = message or string.format('Expected %s but got %s', tostring(expected), tostring(actual))
  assert(actual==expected, message)
end

local function it(message, testFn)
  local status, err =  pcall(testFn)
  if status then
    print(string.format('✓ %s', message))
  else
    print(string.format('✖ %s', message))
    print(string.format('  FAILED: %s', err))
    failures = failures + 1
  end
end


-- ============================================================================
-- stuart.SparkConf
-- ============================================================================

it('works', function()
  local conf = SparkConf.new():setMaster('local'):setAppName('My app')
  assertEqual('local', conf:master())
  assertEqual('My app', conf:appName())
end)


-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

print(string.format('End of test: %d failures', failures))
