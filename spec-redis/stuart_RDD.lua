-- Unit tests derived from https://github.com/BixData/stuart/blob/master/spec/LaTrobe_spec.lua
print('Begin test')

local sc = require 'stuart'.NewContext('local[1]', 'test-stuart_rdd.lua')
local moses = require 'moses'


-- ============================================================================
-- Mini test framework
-- ============================================================================

local failures = 0

local function assertContains(array, expected, message)
  for _,v in pairs(array) do
    if v == expected then return end
  end
  message = message or string.format('Expected array {%s} to contain %s', table.concat(array,','), tostring(expected))
  error(message)
end

local function assertContainsKeyedPair(array, key, value, message)
  for _, e in ipairs(array) do
    if e[1] == key and moses.isEqual(e[2], value) then return end
  end
  local arrayAsStr = ''
  for i,v in ipairs(array) do
    if i > 1 then arrayAsStr = arrayAsStr .. ', ' end
    arrayAsStr = arrayAsStr .. '{' .. v[1] .. ',{' .. v[2][1] .. ',' .. v[2][2] .. '}}'
  end
  message = message or string.format('Expected array {%s} to contain pair {%s,%s} at key %s', arrayAsStr, tostring(value[1]), tostring(value[2]), key)
  error(message)
end

local function assertContainsPair(array, value, message)
  for _, e in ipairs(array) do
    if moses.isEqual(e, value) then return end
  end
  local arrayAsStr = ''
  for i,v in ipairs(array) do
    if i > 1 then arrayAsStr = arrayAsStr .. ', ' end
    arrayAsStr = arrayAsStr .. '{' .. v[1] .. ',{' .. v[2][1] .. ',' .. v[2][2] .. '}}'
  end
  message = message or string.format('Expected array {%s} to contain pair {%s,%s}', arrayAsStr, tostring(value[1]), tostring(value[2]))
  error(message)
end

local function assertIsInRange(n, start, stop)
  local _start = moses.isNil(stop) and 0 or start or 0
  local _stop = moses.isNil(stop) and start or stop or 1
  assert(n >= _start and n < _stop, string.format('Expected %d to be between %d and %d', n, start, stop))
end

local function assertNotContains(array, expected, message)
  for _,v in pairs(array) do
    if v == expected then
      message = message or string.format('Expected array {%s} not to contain %s', table.concat(array,','), tostring(expected))
      error(message)
    end
  end
end

local function assertNotContainsPair(array, value, message)
  for _, e in ipairs(array) do
    if moses.isEqual(e, value) then
      local arrayAsStr = ''
      for i,v in ipairs(array) do
        if i > 1 then arrayAsStr = arrayAsStr .. ', ' end
        arrayAsStr = arrayAsStr .. '{' .. v[1] .. ',{' .. v[2][1] .. ',' .. v[2][2] .. '}}'
      end
      message = message or string.format('Expected array {%s} not to contain pair {%s,%s}', arrayAsStr, tostring(value[1]), tostring(value[2]))
      error(message)
    end
  end
end

local function assertEquals(expected,actual,message)
  message = message or string.format('Expected %s but got %s', tostring(expected), tostring(actual))
  assert(actual==expected, message)
end

local function assertSame(expected,actual,message)
  message = message or string.format('Expected %s but got %s', tostring(expected), tostring(actual))
  assert(moses.isEqual(expected, actual), message)
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

it('assertContains() works', function()
  -- positive test
  assertContains({5,7,9}, 7)
  
  -- negative test
  local status, _ = pcall(function()
    assertContains({'a','b','c'}, 6)
  end)
  assert(status == false)
end)

it('assertContainsKeyedPair() works', function()
  -- positive test
  assertContainsKeyedPair({{'dog',{5,6}}, {'cat',{7,9}}}, 'cat', {7,9})
  
  -- negative test
  local status, _ = pcall(function()
    assertContainsKeyedPair({{'fish',{5,6}}, {'bird',{7,8}}}, 'fish', {7,9})
  end)
  assert(status == false)
end)


-- ============================================================================
-- stuart.RDD
-- ============================================================================

it('aggregate() Examples 1', function()
  local z = sc:parallelize({1,2,3,4,5,6}, 2)
  
  local maxOp = function(x,y) return math.max(x,y) end
  local sumOp = function(x,y) return x+y end
  local res40 = z:aggregate(0, maxOp, sumOp)
  assertEquals(9, res40)
  
  -- This example returns 16 since the initial value is 5
  -- reduce of partition 0 will be max(5, 1, 2, 3) = 5
  -- reduce of partition 1 will be max(5, 4, 5, 6) = 6
  -- final reduce across partitions will be 5 + 5 + 6 = 16
  -- note the final reduce include the initial value
  local res29 = z:aggregate(5, maxOp, sumOp)
  assertEquals(16, res29)
  
  z = sc:parallelize({'a','b','c','d','e','f'},2)
  local concatOp = function(x,y) return x..y end
  local res115 = z:aggregate('', concatOp, concatOp)
  assertEquals('abcdef', res115)

  -- See here how the initial value "x" is applied three times.
  --  - once for each partition
  --  - once when combining all the partitions in the second reduce function.
  local res116 = z:aggregate('x', concatOp, concatOp)
  assertEquals(true, res116 == 'xxdefxabc' or res116 == 'xxabcxdef')
  
  -- Below are some more advanced examples. Some are quite tricky to work out.
  
  z = sc:parallelize({'12','23','345','4567'},2)
  local res141 = z:aggregate('', function(x,y) return tostring(math.max(x:len(), y:len())) end, concatOp)
  assertEquals(true, res141 == '42' or res141 == '24')
  
  local res142 = z:aggregate('', function(x,y) return tostring(math.min(x:len(), y:len())) end, concatOp)
  assertEquals('11', res142)
  
  z = sc:parallelize({'12','23','345',''},2)
  local res143 = z:aggregate('', function(x,y) return tostring(math.min(x:len(), y:len())) end, concatOp)
  assertEquals(true, res143 == '10' or res143 == '01')
end)

it('aggregate() Examples 2', function()
  local z = sc:parallelize({'12','23','','345'}, 2)
  local res144 = z:aggregate('', function(x,y) return tostring(math.min(x:len(), y:len())) end, function(x,y) return x..y end)
  assertEquals('11', res144)
end)

it('aggregateByKey()', function()
  local pairRDD = sc:parallelize({{'cat',2}, {'cat',5}, {'mouse',4}, {'cat',12}, {'dog',12}, {'mouse',2}}, 2)

  local seqOp = function(x,y) return math.max(x,y) end
  local combOp = function(x,y) return x+y end

  local res3 = pairRDD:aggregateByKey(0, seqOp, combOp):collect()
  assertContainsPair(res3, {'dog',12})
  assertContainsPair(res3, {'cat',17})
  assertContainsPair(res3, {'mouse',6})

  local res4 = pairRDD:aggregateByKey(100, seqOp, combOp):collect()
  assertContainsPair(res4, {'dog',100})
  assertContainsPair(res4, {'cat',200})
  assertContainsPair(res4, {'mouse',200})
end)

it('cartesian()', function()
  local x = sc:parallelize({1,2,3,4,5})
  local y = sc:parallelize({6,7,8,9,10})
  local actual = x:cartesian(y):collect()
  assertContainsPair(actual, {1,6})
  assertContainsPair(actual, {1,7})
  assertContainsPair(actual, {1,8})
  assertContainsPair(actual, {1,9})
  assertContainsPair(actual, {1,10})
  assertContainsPair(actual, {2,6})
  assertContainsPair(actual, {2,7})
  assertContainsPair(actual, {2,8})
  assertContainsPair(actual, {2,9})
  assertContainsPair(actual, {2,10})
  assertContainsPair(actual, {3,6})
  assertContainsPair(actual, {3,7})
  assertContainsPair(actual, {3,8})
  assertContainsPair(actual, {3,9})
  assertContainsPair(actual, {3,10})
  assertContainsPair(actual, {4,6})
  assertContainsPair(actual, {4,7})
  assertContainsPair(actual, {4,8})
  assertContainsPair(actual, {4,9})
  assertContainsPair(actual, {4,10})
  assertContainsPair(actual, {5,6})
  assertContainsPair(actual, {5,7})
  assertContainsPair(actual, {5,8})
  assertContainsPair(actual, {5,9})
  assertContainsPair(actual, {5,10})
end)

it('coalesce()', function()
  local y = sc:parallelize(moses.range(1,10), 10)
  local z = y:coalesce(2, false)
  assertEquals(2, #z.partitions)
end)

--it('combineByKey()', function()
--  local a = sc:parallelize({'dog', 'cat', 'gnu', 'salmon', 'rabbit', 'turkey', 'wolf', 'bear', 'bee'}, 3)
--  local b = sc:parallelize({1,1,2,2,2,1,2,2,2}, 3)
--  local c = b:zip(a)
--  local createCombiner = function(v) return {v} end
--  local mergeValue = function(x, y) x[#x+1] = y; return x end
--  local mergeCombiners = function(x, y) return moses.append(x, y) end
--  local d = c:combineByKey(createCombiner, mergeValue, mergeCombiners)
--  local actual = d:collect()
--
--  local valuesForKey1 = moses.reduce(actual, function(r,e) if e[1]==1 then r=e[2] end; return r end, {})
--  assertContains(valuesForKey1, 'cat')
--  assertContains(valuesForKey1, 'dog')
--  assertContains(valuesForKey1, 'turkey')
--
--  local valuesForKey2 = moses.reduce(actual, function(r,e) if e[1]==2 then r=e[2] end; return r end, {})
--  assertContains(valuesForKey2, 'gnu')
--  assertContains(valuesForKey2, 'rabbit')
--  assertContains(valuesForKey2, 'salmon')
--  assertContains(valuesForKey2, 'bee')
--  assertContains(valuesForKey2, 'bear')
--  assertContains(valuesForKey2, 'wolf')
--end)

it('context, sparkContext', function()
  local c = sc:parallelize({'Gnu', 'Cat', 'Rat', 'Dog'}, 2)
  assertEquals(sc, c.context)
  assertEquals(sc, c.sparkContext)
end)

it('count()', function()
  local c = sc:parallelize({'Gnu', 'Cat', 'Rat', 'Dog'})
  assertEquals(4, c:count())
end)

it('countByKey()', function()
  local c = sc:parallelize({{3,'Gnu'}, {3,'Yak'}, {5,'Mouse'}, {3,'Dog'}})
  local actual = c:countByKey()
  assertEquals(3, actual[3])
  assertEquals(1, actual[5])
end)

it('countByValue()', function()
  local b = sc:parallelize({1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1})
  local actual = b:countByValue()
  assertEquals(1, actual[5])
  assertEquals(1, actual[8])
  assertEquals(1, actual[3])
  assertEquals(1, actual[8])
  assertEquals(1, actual[6])
  assertEquals(6, actual[1])
  assertEquals(3, actual[2])
  assertEquals(2, actual[4])
  assertEquals(1, actual[7])
end)
  
it('distinct()', function()
  local c = sc:parallelize({'Gnu', 'Cat', 'Rat', 'Dog', 'Gnu', 'Rat'})
  local actual = c:distinct():collect()
  assertEquals(4, #actual)
end)

it('filter() without mixed data', function()
  local a = sc:parallelize({1,2,3,4,5,6,7,8,9,10}, 3)
  local b = a:filter(function(x) return x % 2 == 0 end)
  local actual = b:collect()
  assertEquals(true, moses.isEqual({2,4,6,8,10}, actual))
end)

it('filter() with mixed data', function()
  local b = sc:parallelize(moses.range(1,8))
  local actual = b:filter(function(x) return x < 4 end):collect()
  assertEquals(true, moses.isEqual({1,2,3}, actual))

  local a = sc:parallelize({'cat', 'horse', 4.0, 3.5, 2, 'dog'})
  local status, _ = pcall(function()
    a:filter(function(x) return x < 4 end):collect()
  end)
  assertEquals(false, status, 'expected filter with mixed data to cause error')
end)

it('first()', function()
  local c = sc:parallelize({'Gnu', 'Cat', 'Rat', 'Dog'})
  assertEquals('Gnu', c:first())
end)

it('flatMap()', function()
  local a = sc:parallelize({1,2,3,4,5,6,7,8,9,10}, 5)
  local actual = a:flatMap(function(x) return moses.range(1,x) end):collect()
  local expected = {1, 1, 2, 1, 2, 3, 1, 2, 3, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
  assertSame(expected, actual)

  actual = sc:parallelize({1,2,3}, 2):flatMap(function(x) return {x,x,x} end):collect()
  expected = {1, 1, 1, 2, 2, 2, 3, 3, 3}
  assertSame(expected, actual)
end)

it('flatMapValues()', function()
  local a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'panther', 'eagle'}, 2)
  local b = a:map(function(x) return {string.len(x), x} end)
  local actual = b:flatMapValues(function(x) return 'x' .. x .. 'x' end):collect()
  local expected = {
    {3,'x'}, {3,'d'}, {3,'o'}, {3,'g'}, {3,'x'}, {5,'x'}, {5,'t'}, {5,'i'},
    {5,'g'}, {5,'e'}, {5,'r'}, {5,'x'}, {4,'x'}, {4,'l'}, {4,'i'}, {4,'o'},
    {4,'n'}, {4,'x'}, {3,'x'}, {3,'c'}, {3,'a'}, {3,'t'}, {3,'x'}, {7,'x'},
    {7,'p'}, {7,'a'}, {7,'n'}, {7,'t'}, {7,'h'}, {7,'e'}, {7,'r'}, {7,'x'},
    {5,'x'}, {5,'e'}, {5,'a'}, {5,'g'}, {5,'l'}, {5,'e'}, {5,'x'}
  }
  assertSame(expected, actual)
end)

it('fold()', function()
  local a = sc:parallelize({1, 2, 3}, 3)
  local actual = a:fold(0, function(b,c) return b+c end)
  assertEquals(6, actual)
end)

it('foldByKey()', function()
  local a = sc:parallelize({'dog', 'cat', 'owl', 'gnu', 'ant'}, 2)
  local b = a:map(function(x) return {string.len(x), x} end)
  local actual = b:foldByKey('', function(c,d) return c .. d end):collect()
  assertSame({{3,'dogcatowlgnuant'}}, actual)
end)

it('foreach()', function()
  local c = sc:parallelize({'cat', 'dog', 'tiger', 'lion', 'gnu', 'crocodile', 'ant', 'whale', 'dolphin', 'spider'}, 3)
  local r = {}
  c:foreach(function(x) table.insert(r, x .. 's are yummy') end)
  local expected = {
    'cats are yummy',
    'dogs are yummy',
    'tigers are yummy',
    'lions are yummy',
    'gnus are yummy',
    'crocodiles are yummy',
    'ants are yummy',
    'whales are yummy',
    'dolphins are yummy',
    'spiders are yummy',
  }
  assertSame(expected, r)
end)

it('foreachPartition()', function()
  local b = sc:parallelize({1,2,3,4,5,6,7,8,9}, 3)
  local actual = {}
  b:foreachPartition(function(x)
    local v = moses.reduce(x, function(r,a) return r+a end, 0)
    table.insert(actual, v)
  end)
  assertSame({6,15,24}, actual)
end)

it('glom()', function()
  local a = sc:parallelize(moses.range(1,100), 3)
  local actual = a:glom():collect()
  assertEquals(3, #actual)
  local values = moses.flatten(actual)
  for i = 1, 100 do
    assertContains(values, i)
  end
end)

it('groupBy()', function()
  local a = sc:parallelize(moses.range(1,9), 3)
  local actual = a:groupBy(function(x)
    if x % 2 == 0 then return 'even' else return 'odd' end
  end):collect()
  assertContainsKeyedPair(actual, 'even', {2,4,6,8})
  assertContainsKeyedPair(actual, 'odd', {1,3,5,7,9})
end)

it('groupByKey()', function()
  local a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'spider', 'eagle'}, 2)
  local b = a:keyBy(function(x) return string.len(x) end)
  assertSame({{3,'dog'}, {5,'tiger'}, {4,'lion'}, {3,'cat'}, {6,'spider'}, {5,'eagle'}}, b:collect(), 'pre-condition sanity check')
  local actual = b:groupByKey():collect()
  assertContainsKeyedPair(actual, 4, {'lion'})
  assertContainsKeyedPair(actual, 6, {'spider'})
  assertContainsKeyedPair(actual, 3, {'dog','cat'})
  assertContainsKeyedPair(actual, 5, {'tiger','eagle'})
end)

it('histogram() with even spacing', function()
  local a = sc:parallelize({1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0}, 3)
  local buckets, counts = a:histogram(5)
  assertSame({1.1, 2.68, 4.26, 5.84, 7.42, 9.0}, buckets)
  assertSame({5, 0, 0, 1, 4}, counts)

  a = sc:parallelize({9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5}, 3)
  buckets, counts = a:histogram(6)
  assertSame({1.0, 2.5, 4.0, 5.5, 7.0, 8.5, 10.0}, buckets)
  assertSame({6, 0, 1, 1, 3, 4}, counts)
end)

it('histogram() with custom spacing', function()
  local a = sc:parallelize({1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0}, 3)
  local counts = a:histogram({0.0, 3.0, 8.0})
  assertSame({5, 3}, counts)

  a = sc:parallelize({9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5}, 3)
  counts = a:histogram({0.0, 5.0, 10.0})
  assertSame({6, 9}, counts)

  counts = a:histogram({0.0, 5.0, 10.0, 15.0})
  assertSame({6, 8, 1}, counts)
end)

it('id', function()
  local y = sc:parallelize(moses.range(1,10), 10)
  assertEquals(true, y.id > 0)
end)

it('intersection()', function()
  local x = sc:parallelize(moses.range(1,20))
  local y = sc:parallelize(moses.range(10,30))
  local z = x:intersection(y)
  local actual = z:collect()
  local expected = {16, 12, 20, 13, 17, 14, 18, 10, 19, 15, 11}
  table.sort(actual)
  table.sort(expected)
  assertSame(expected, actual)
end)

-- no LaTrobe spec for isEmpty, this is authored here
it('isEmpty()', function()
  assertEquals(true, sc:parallelize({}):isEmpty())
  assertEquals(false, sc:parallelize({1}):isEmpty())
end)

it('join()', function()
  local a = sc:parallelize({'dog', 'salmon', 'salmon', 'rat', 'elephant'}, 3)
  local b = a:keyBy(function(x) return string.len(x) end)
  local c = sc:parallelize({'dog', 'cat', 'gnu', 'salmon', 'rabbit', 'turkey', 'wolf', 'bear', 'bee'}, 3)
  local d = c:keyBy(function(x) return string.len(x) end)
  local actual = b:join(d):collect()
  assertContainsKeyedPair(actual, 6, {'salmon', 'salmon'})
  assertContainsKeyedPair(actual, 6, {'salmon', 'rabbit'})
  assertContainsKeyedPair(actual, 6, {'salmon', 'turkey'})
  assertContainsKeyedPair(actual, 3, {'dog', 'dog'})
  assertContainsKeyedPair(actual, 3, {'dog', 'cat'})
  assertContainsKeyedPair(actual, 3, {'dog', 'gnu'})
  assertContainsKeyedPair(actual, 3, {'dog', 'bee'})
  assertContainsKeyedPair(actual, 3, {'rat', 'dog'})
  assertContainsKeyedPair(actual, 3, {'rat', 'cat'})
  assertContainsKeyedPair(actual, 3, {'rat', 'gnu'})
  assertContainsKeyedPair(actual, 3, {'rat', 'bee'})
end)

it('keyBy()', function()
  local a = sc:parallelize({'dog', 'salmon', 'salmon', 'rat', 'elephant'})
  local b = a:keyBy(function(x) return string.len(x) end)
  local actual = b:collect()
  local expected = {
    {3,'dog'},
    {6,'salmon'},
    {6,'salmon'},
    {3,'rat'},
    {8,'elephant'}
  }
  assertSame(expected, actual)
end)

it('keys()', function()
  local a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'panther', 'eagle'}, 2)
  local b = a:map(function(x) return {string.len(x), x} end)
  local actual = b:keys():collect()
  local expected = {3,5,4,3,7,5}
  assertSame(expected, actual)
end)

it('leftOuterJoin()', function()
  local a = sc:parallelize({'dog', 'salmon', 'salmon', 'rat', 'elephant'}, 3)
  local b = a:keyBy(function(x) return string.len(x) end)
  local c = sc:parallelize({'dog', 'cat', 'gnu', 'salmon', 'rabbit', 'turkey', 'wolf', 'bear', 'bee'}, 3)
  local d = c:keyBy(function(x) return string.len(x) end)
  local actual = b:leftOuterJoin(d):collect()
  assertContainsKeyedPair(actual, 6, {'salmon', 'salmon'})
  assertContainsKeyedPair(actual, 6, {'salmon', 'rabbit'})
  assertContainsKeyedPair(actual, 6, {'salmon', 'turkey'})
  assertContainsKeyedPair(actual, 3, {'dog', 'dog'})
  assertContainsKeyedPair(actual, 3, {'dog', 'cat'})
  assertContainsKeyedPair(actual, 3, {'dog', 'gnu'})
  assertContainsKeyedPair(actual, 3, {'dog', 'bee'})
  assertContainsKeyedPair(actual, 3, {'rat', 'dog'})
  assertContainsKeyedPair(actual, 3, {'rat', 'cat'})
  assertContainsKeyedPair(actual, 3, {'rat', 'gnu'})
  assertContainsKeyedPair(actual, 3, {'rat', 'bee'})
  assertContainsKeyedPair(actual, 8, {'elephant', nil})
end)

it('lookup()', function()
  local a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'panther', 'eagle'}, 2)
  local b = a:map(function(x) return {string.len(x), x} end)
  local actual = b:lookup(5)
  assertContains(actual, 'tiger')
  assertContains(actual, 'eagle')
end)

it('mapPartitions() Example 1', function()
  local a = sc:parallelize(moses.range(1,9), 3)
  local myfunc = function(iter)
    local res = {}
    local pre
    for cur in iter do
      if pre ~= nil then res[#res+1] = {pre, cur} end
      pre = cur
    end
    local i = 0
    return function()
      i = i + 1
      if i <= #res then return res[i] end
    end
  end
  local actual = a:mapPartitions(myfunc):collect()
  assertContainsPair(actual, {2,3})
  assertContainsPair(actual, {1,2})
  assertContainsPair(actual, {5,6})
  assertContainsPair(actual, {4,5})
  assertContainsPair(actual, {8,9})
  assertContainsPair(actual, {7,8})
  assertNotContainsPair(actual, {3,4})
  assertNotContainsPair(actual, {6,7})
end)

-- value 10 removed, removing non-determinism caused by bucket splits
it('mapPartitionsWithIndex()', function()
  local a = sc:parallelize({1,2,3,4,5,6,7,8,9}, 3)
  local myfunc = function(index, iter)
    local res = {}
    for x in iter do res[#res+1] = index .. ',' .. x end
    local i = 0
    return function()
      i = i + 1
      if i <= #res then return res[i] end
    end
  end
  local actual = a:mapPartitionsWithIndex(myfunc):collect()
  assertContains(actual, '0,1')
  assertContains(actual, '0,2')
  assertContains(actual, '0,3')
  assertContains(actual, '1,4')
  assertContains(actual, '1,5')
  assertContains(actual, '1,6')
  assertContains(actual, '2,7')
  assertContains(actual, '2,8')
  assertContains(actual, '2,9')
end)

it('mapValues()', function()
  local a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'panther', 'eagle'})
  local b = a:map(function(x) return {string.len(x), x} end)
  local actual = b:mapValues(function(x) return 'x' .. x .. 'x' end):collect()
  local expected = {
    {3, 'xdogx'},
    {5, 'xtigerx'},
    {4, 'xlionx'},
    {3, 'xcatx'},
    {7, 'xpantherx'},
    {5, 'xeaglex'}
  }
  assertSame(expected, actual)
end)

it('max()', function()
  local y = sc:parallelize(moses.range(10,30))
  assertEquals(30, y:max())
end)

it('mean()', function()
  local a = sc:parallelize({9.1,1.0,1.2,2.1,1.3,5.0,2.0,2.1,7.4,7.5,7.6,8.8,10.0,8.9,5.5}, 3)
  local round = function(num, decimalPlaces)
    local mult = 10 ^ (decimalPlaces or 0)
    return math.floor(num * mult + 0.5) / mult
  end
  assertEquals(5.3, round(a:mean(), 1))
end)

it('min()', function()
  local y = sc:parallelize(moses.range(10,30))
  assertEquals(10, y:min())
end)

it('reduce()', function()
  local a = sc:parallelize(moses.range(1,100), 3)
  local actual = a:reduce(function(r,x) return r+x end)
  assertEquals(5050, actual)
end)

it('reduceByKey()', function()
  local a = sc:parallelize({'dog', 'cat', 'owl', 'gnu', 'ant'}, 2)
  local b = a:map(function(x) return {string.len(x), x} end)
  local res86 = b:reduceByKey(function(r,x) return r .. x end):collect()
  local expected = {{3,'dogcatowlgnuant'}}
  assertSame(expected, res86)

  a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'panther', 'eagle'}, 2)
  b = a:map(function(x) return {string.len(x), x} end)
  local res87 = b:reduceByKey(function(r, x) return r .. x end):collect()
  assertContainsKeyedPair(res87, 4, 'lion')
  assertContainsKeyedPair(res87, 3, 'dogcat')
  assertContainsKeyedPair(res87, 7, 'panther')
  assertContainsKeyedPair(res87, 5, 'tigereagle')
end)

it('repartition()', function()
  local rdd = sc:parallelize({1,2,10,4,5,2,1,1,1}, 3)
  assertEquals(3, #rdd.partitions)
  local rdd2 = rdd:repartition(5)
  assertEquals(5, #rdd2.partitions)
end)

it('name, setName()', function()
  local y = sc:parallelize(moses.range(1,10), 10)
  assertEquals(nil, y.name)
  y:setName("Fancy RDD Name")
  assertEquals("Fancy RDD Name", y.name)
end)

it('partitions', function()
  local b = sc:parallelize({'gnu', 'cat', 'rat', 'dog', 'gnu', 'rat'}, 2)
  local partitions = b.partitions
  assertEquals(true, moses.isTable(partitions))
  assertEquals(2, #partitions)
end)

it('sortBy()', function()
  local y = sc:parallelize({5, 7, 1, 3, 2, 1})

  local rdd = y:sortBy(moses.identity, true)
  assertSame({1, 1, 2, 3, 5, 7}, rdd:collect())

  rdd = y:sortBy(moses.identity, false)
  assertSame({7, 5, 3, 2, 1, 1}, rdd:collect())

  local z = sc:parallelize({{'H',10}, {'A',26}, {'Z',1}, {'L',5}})
  rdd = z:sortBy(function(e) return e[1] end, true)
  assertSame({{'A',26}, {'H',10}, {'L',5}, {'Z',1}}, rdd:collect())

  rdd = z:sortBy(function(e) return e[2] end, true)
  assertSame({{'Z',1}, {'L',5}, {'H',10}, {'A',26}}, rdd:collect())
end)

-- Enable once moses.zip is transpiled
--it('sortByKey() [Ordered]', function()
--  local a = sc:parallelize({'dog','cat','owl','gnu','ant'}, 2)
--  local b = sc:parallelize(moses.range(1, a:count()), 2)
--  local c = a:zip(b)
--  local res74 = c:sortByKey(true):collect()
--  assertSame({{'ant',5}, {'cat',2}, {'dog',1}, {'gnu',4}, {'owl',3}}, res74)
--  local res75 = c:sortByKey(false):collect()
--  assertSame({{'owl',3}, {'gnu',4}, {'dog',1}, {'cat',2}, {'ant',5}}, res75)
--end)

it('stats()', function()
  local x = sc:parallelize({1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0}, 2)
  local res16 = x:stats()
  assertEquals(9, res16.count)
  assertIsInRange(res16.mean, 11.26, 11.27)
  assertIsInRange(res16.stdev, 8.619836135, 8.619836137)
end)

it('stdev()', function()
  local d = sc:parallelize({0.0, 0.0, 0.0}, 3)
  local res10 = d:stdev()
  assertEquals(0.0, res10)

  d = sc:parallelize({0.0, 1.0}, 3)
  local res18 = d:stdev()
  local res19 = d:sampleStdev()
  assertEquals(0.5, res18)
  assertIsInRange(res19, 0.70710678, 0.70710679)

  d = sc:parallelize({0.0, 0.0, 1.0}, 3)
  local res14 = d:stdev()
  assertEquals(0.4714045207910317, res14)
end)

it('subtract()', function()
  local a = sc:parallelize(moses.range(1,9), 3)
  local b = sc:parallelize(moses.range(1,3), 3)
  local c = a:subtract(b)
  local actual = c:collect()
  assertNotContains(actual, 1)
  assertNotContains(actual, 2)
  assertNotContains(actual, 3)
  assertContains(actual, 4)
  assertContains(actual, 5)
  assertContains(actual, 6)
  assertContains(actual, 7)
  assertContains(actual, 8)
  assertContains(actual, 9)
end)

it('subtractByKey()', function()
  local a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'spider', 'eagle'}, 2)
  local b = a:keyBy(function(x) return string.len(x) end)
  local c = sc:parallelize({'ant', 'falcon', 'squid'}, 2)
  local d = c:keyBy(function(x) return string.len(x) end)
  local result = b:subtractByKey(d):collect()
  assertContainsPair(result, {4,'lion'})
end)

it('take()', function()
  local b = sc:parallelize({'dog', 'cat', 'ape', 'salmon', 'gnu'}, 2)
  local actual = b:take(2)
  assertSame({'dog', 'cat'}, actual)
end)

it('toLocalIterator()', function()
  local rdd = sc:parallelize({2,3}, 1)
  local iter = rdd:toLocalIterator()
  assertEquals(2, iter())
  assertEquals(3, iter())
  assertEquals(nil, iter())
end)

it('top()', function()
  local c = sc:parallelize({6, 9, 4, 7, 5, 8}, 2)
  assertSame({9,8}, c:top(2))
end)

it('toString()', function()
  local z = sc:parallelize({1,2,3,4,5,6}, 2)
  local actual = z:toString()
  assertEquals(true, string.find(actual, 'RDD') ~= nil)
  assertEquals(true, string.find(actual, '[' .. z.id .. ']') ~= nil)
end)

it('treeAggregate()', function()
  local z = sc:parallelize({1,2,3,4,5,6}, 2)
  
  -- lets first print out the contents of the RDD with partition labels
  local myfunc = function(index, iter)
    local res = {}
    for x in iter do
      res[#res+1] = string.format('[partID:%d, val: %d]', index, x)
    end
    local i = 0
    return function()
      i = i + 1
      if i <= #res then return res[i] end
    end
  end
  local res28 = z:mapPartitionsWithIndex(myfunc):collect()
  assertContains(res28, '[partID:0, val: 1]')
  assertContains(res28, '[partID:0, val: 2]')
  assertContains(res28, '[partID:0, val: 3]')
  assertContains(res28, '[partID:1, val: 4]')
  assertContains(res28, '[partID:1, val: 5]')
  assertContains(res28, '[partID:1, val: 6]')
  
  local seqOp = function(x,y) return math.max(x,y) end
  local combOp = function(x,y) return x+y end
  local res40 = z:treeAggregate(0, seqOp, combOp)
  assertEquals(9, res40)
  
  -- Note unlike normal aggregrate. Tree aggregate does not apply the initial value for the second reduce
  -- This example returns 11 since the initial value is 5
  -- reduce of partition 0 will be max(5, 1, 2, 3) = 5
  -- reduce of partition 1 will be max(4, 5, 6) = 6
  -- final reduce across partitions will be 5 + 6 = 11
  -- note the final reduce does not include the initial value
  local res42 = z:treeAggregate(5, seqOp, combOp)
  assertEquals(11, res42)
end)

it('treeReduce()', function()
  local z = sc:parallelize({1,2,3,4,5,6}, 2)
  local res49 = z:treeReduce(function(x,y) return x+y end)
  assertEquals(21, res49)
end)

it('union()', function()
  local a = sc:parallelize(moses.range(1,3), 1)
  local b = sc:parallelize(moses.range(5,7), 1)
  local actual = a:union(b):collect()
  assertSame({1,2,3,5,6,7}, actual)
end)

it('values()', function()
  local a = sc:parallelize({'dog', 'tiger', 'lion', 'cat', 'panther', 'eagle'}, 2)
  local b = a:map(function(x) return {string.len(x), x} end)
  local actual = b:values():collect()
  assertEquals(true, moses.isEqual({'dog', 'tiger', 'lion', 'cat', 'panther', 'eagle'}, actual))
end)

it('variance()', function()
  local a = sc:parallelize({9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9, 5.5}, 3)
  local res70 = a:variance()
  assertIsInRange(res70, 10.60533333, 10.60533334)

  local x = sc:parallelize({1.0, 2.0, 3.0, 5.0, 20.0, 19.02, 19.29, 11.09, 21.0}, 2)
  local res14 = x:variance()
  assertIsInRange(res14, 66.04584444, 66.04584445)
  
  local res13 = x:sampleVariance()
  assertIsInRange(res13, 74.30157, 74.30158)
end)

it('zip()', function()
  local a = sc:parallelize(moses.range(1,100), 3)
  local b = sc:parallelize(moses.range(101,200), 3)
  local actual = a:zip(b):collect()
  assertContainsPair(actual, {1,101})
  assertContainsPair(actual, {2,102})
  assertContainsPair(actual, {99,199})
  assertContainsPair(actual, {100,200})
end)

it('zipWithIndex()', function()
  local z = sc:parallelize({'A','B','C','D'})
  local r = z:zipWithIndex():collect()
  assertContainsPair(r, {'A',0})
  assertContainsPair(r, {'B',1})
  assertContainsPair(r, {'C',2})
  assertContainsPair(r, {'D',3})

  z = sc:parallelize(moses.range(100,120), 5)
  r = z:zipWithIndex():collect()
  assertContainsPair(r, {100,0})
  assertContainsPair(r, {101,1})
  assertContainsPair(r, {102,2})
  assertContainsPair(r, {119,19})
  assertContainsPair(r, {120,20})
end)

-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

print(string.format('End of test: %d failures', failures))
