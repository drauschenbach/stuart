print('Begin test')

local Partition = require 'stuart.Partition'


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
-- stuart.Partition
-- ============================================================================

it('new()', function()
  local p = Partition.new({1,2,3}, 4)
  assertEqual(3, #p.data)
  assertEqual(1, p.data[1])
  assertEqual(2, p.data[2])
  assertEqual(3, p.data[3])
  assertEqual(4, p.index)
end)

it('_count()', function()
  local p = Partition.new({1,2,3,4,5})
  assertEqual(5, p:_count())
end)

-- https://github.com/Yonaba/Moses/blob/master/doc/tutorial.md#flatten-array--shallow--false
it('_flatten()', function()
  local p = Partition.new({1,{2,3},{4,5,{6,7}}})
  p:_flatten()
  assertEqual(7, p:_count())
  assertEqual(1, p.data[1])
  assertEqual(2, p.data[2])
  assertEqual(3, p.data[3])
  assertEqual(4, p.data[4])
  assertEqual(5, p.data[5])
  assertEqual(6, p.data[6])
  assertEqual(7, p.data[7])
end)

--TODO
--it('_flattenValues()', function()
--end

it('_toLocalIterator()', function()
  local p = Partition.new({5,6})
  local iter = p:_toLocalIterator()
  assertEqual(5, iter())
  assertEqual(6, iter())
  assertEqual(nil, iter())
end)

-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

print(string.format('End of test: %d failures', failures))
