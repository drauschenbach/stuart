print('Begin test')

local stuart = require 'stuart'


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
-- Stuart
-- ============================================================================

it('NewContext()', function()
  local sc = stuart.NewContext('local[1]', 'test.lua')
  assertEqual('test.lua', sc:appName())
end)

it('NewStreamingContext()', function()
  local sc = stuart.NewContext('local[1]', 'test.lua')
  local ssc = stuart.NewStreamingContext(sc, 0.1)
  assertEqual(0.1, ssc.batchDuration)
end)

-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

print(string.format('End of test: %d failures', failures))
