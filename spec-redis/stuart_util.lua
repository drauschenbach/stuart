print('Begin test')

local util = require 'stuart.util'


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
-- stuart.util
-- ============================================================================

it('jsonDecode() can decode a scalar', function()
  local actual = util.jsonDecode('7')
  assertEqual(7, actual)
end)

it('jsonDecode() can decode an object', function()
  local actual = util.jsonDecode('{"a":7,"b":"F"}')
  assertEqual(7, actual['a'])
  assertEqual('F', actual['b'])
end)

it('split()', function()
  local actual = util.split('one,two,three', ',')
  assertEqual(3, #actual)
  assertEqual('one', actual[1])
  assertEqual('two', actual[2])
  assertEqual('three', actual[3])
end)


-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

print(string.format('End of test: %d failures', failures))
