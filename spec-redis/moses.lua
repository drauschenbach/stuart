-- Unit tests derived from https://github.com/Yonaba/Moses/blob/Moses-2.1.0-1/doc/tutorial.md
print('Begin test')

local moses = require 'moses'


-- ============================================================================
-- Mini test framework
-- ============================================================================

local failures = 0

local function assertContains(array, expected, message)
  for _,v in ipairs(array) do
    if moses.isEqual(v, expected) then return end
  end
  message = message or string.format('Expected array {%s} to contain %s', table.concat(array,','), tostring(expected))
  error(message)
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
  local status, err = pcall(testFn)
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


-- ============================================================================
-- Operators
-- ============================================================================

it('operator.add()', function()
  assertEquals(-2, moses.operator.add(5,-7))
end)

it('operator.concat()', function()
  assertEquals('abc123', moses.operator.concat('abc','123'))
end)

it('operator.div()', function()
  assertEquals(2.5, moses.operator.div(5,2))
end)

it('operator.eq()', function()
  assertEquals(true, moses.operator.eq(2,2))
  assertEquals(false, moses.operator.eq(2,3))
end)

it('operator.exp()', function()
  assertEquals(343, moses.operator.exp(7,3))
end)

it('operator.floordiv()', function()
  -- TODO
end)

it('operator.ge()', function()
  assertEquals(true, moses.operator.ge(5,5))
  assertEquals(true, moses.operator.ge(7,5))
  assertEquals(false, moses.operator.ge(5,7))
end)

it('operator.gt()', function()
  assertEquals(false, moses.operator.gt(5,5))
  assertEquals(true, moses.operator.gt(7,5))
end)

it('operator.intdiv()', function()
  -- TODO
end)

it('operator.land()', function()
  assertEquals(true, moses.operator.land(true,true))
  assertEquals(false, moses.operator.land(true,false))
end)

it('operator.le()', function()
  assertEquals(true, moses.operator.le(5,5))
  assertEquals(true, moses.operator.le(5,7))
  assertEquals(false, moses.operator.le(7,5))
end)
  
it('operator.length()', function()
  assertEquals(4, moses.operator.length({1,2,3,4}))
end)

it('operator.lnot()', function()
  -- TODO
end)

it('operator.lor()', function()
  -- TODO
end)

it('operator.lt()', function()
  -- TODO
end)

it('operator.mod()', function()
  -- TODO
end)

it('operator.mul()', function()
  assertEquals(21, moses.operator.mul(7,3))
end)

it('operator.neq()', function()
  assertEquals(true, moses.operator.neq(1,2))
  assertEquals(false, moses.operator.neq(1,1))
end)

it('operator.sub()', function()
  assertEquals(12, moses.operator.sub(15,3))
end)
  
it('operator.unm()', function()
  -- TODO
end)


-- ============================================================================
-- Moses functions
-- ============================================================================

it('all(t, f)', function()
  local function isEven(v) return v%2==0 end
  assertEquals(true, moses.all({2,4,6}, isEven))
end)

it('append()', function()
  local res = moses.append({1,2,3},{'a','b'})
  assertEquals(true, moses.isEqual(res, {1,2,3,'a','b'}))
end)

it('clone({})', function()
  local obj = {}
  local obj2 = moses.clone(obj)
  assertEquals(0, #obj2)
end)

it('clone(obj)', function()
  local obj = {1,2,3}
  local obj2 = moses.clone(obj)
  assertEquals(true, obj ~= obj2)
  assertEquals(true, moses.isEqual(obj2, obj))
end)

it('count(t, val)', function()
  assertEquals(2, moses.count({1,1,2,3,3,3,2,4,3,2}, 1))
  assertEquals(3, moses.count({1,1,2,3,3,3,2,4,3,2}, 2))
  assertEquals(4, moses.count({1,1,2,3,3,3,2,4,3,2}, 3))
  assertEquals(2, moses.count({false,false,true}), false)
  assertEquals(1, moses.count({false,false,true}), true)
end)

it('countf(t, f)', function()
  assertEquals(3, moses.countf({1,2,3,4,5,6}, function(v)
    return v%2==0
  end))
  assertEquals(4, moses.countf({print, pairs, assert, ipairs}, function(v)
    return type(v)=='function' or type(v)=='lightfunction'
  end))
end)

it('detect(t, value)', function()
  assertEquals(2, moses.detect({6,8,10,16},8))
  assertEquals(nil, moses.detect({nil,true,0,true,true},false))
  
  local complex_table = {18,{2,6}}
  local collection = {6,{18,{2,6}},10,{18,{2,{3}}},29}
  assertEquals(2, moses.detect(collection, complex_table))
end)

it('difference(array1, array2)', function()
  local array = {1,2,'a',4,5}
  assertSame({2,4,5}, moses.difference(array,{1,'a'}))
end)

it('each(t,f)', function()
  local res = {}
  moses.each({4,2,1}, function(x) res[#res+1] = x end)
  assertEquals(3, #res)
  assertEquals(4, res[1])
  assertEquals(2, res[2])
  assertEquals(1, res[3])
end)

it('fill() replacing all', function()
  local array = moses.range(1,5)
  assertSame({0,0,0,0,0}, moses.fill(array, 0))
end)

it('fill() starting at index', function()
  local array = moses.range(1,5)
  assertSame({1,2,0,0,0}, moses.fill(array,0,3))
end)

it('fill() replace within range', function()
  local array = moses.range(1,5)
  assertSame({1,0,0,0,5}, moses.fill(array,0,2,4))
end)

it('fill() can enlarge the array', function()
  local array = moses.range(1,5)
  assertSame({1,2,3,4,0,0,0,0,0,0}, moses.fill(array,0,5,10))
end)

it('find(array, value)', function()
  local value = {3}
  local res = moses.find({{4},{3},{2},{1}}, value)
  assertEquals(2, res)
end)

it('find(array, value, from)', function()
  local res = moses.find({1,4,2,3,4,5}, 4,3)
  assertEquals(5, res)
end)

it('first(array, n)', function()
  local array = {1,2,3,4,5,6,7,8,9}
  local res = moses.first(array,3)
  assertSame({1,2,3}, res)
end)

it('flatten()', function()
  local res = moses.flatten({1,{2,3},{4,5,{6,7}}})
  assertEquals(7, #res)
  assertEquals(1, res[1])
  assertEquals(2, res[2])
  assertEquals(3, res[3])
  assertEquals(4, res[4])
  assertEquals(5, res[5])
  assertEquals(6, res[6])
  assertEquals(7, res[7])
end)

it('identity()', function()
  assertEquals(7, moses.identity(7))
  assertEquals('A', moses.identity('A'))
  assertEquals(nil, moses.identity())
end)

it('include()', function()
  assertEquals(true, moses.include({6,8,10,16,29},16))
  assertEquals(false, moses.include({6,8,10,16,29},1))
  
  local complex_table = {18,{2,{3}}}
  local collection = {6,{18,{2,6}},10,{18,{2,{3}}},29}
  assertEquals(true, moses.include(collection, complex_table))
end)

it('indexOf()', function()
  assertEquals(2, moses.indexOf({1,2,3},2))
end)

it('intersection()', function()
  local A = {'a'}
  local B = {'a',1,2,3}
  local C = {2,10,1,'a'}
  assertSame({'a'}, moses.intersection(A,B,C))
end)

it('isBoolean()', function()
  assertEquals(true, moses.isBoolean(true))
  assertEquals(true, moses.isBoolean(false))
  assertEquals(true, moses.isBoolean(1==1))
  assertEquals(false, moses.isBoolean(print))
end)

it('isCallable()', function()
  assertEquals(true, moses.isCallable(print))
  assertEquals(true, moses.isCallable(function() end))
  assertEquals(true, moses.isCallable(setmetatable({}, {__index=string}).upper))
  assertEquals(true, moses.isCallable(setmetatable({}, {__call=function() return end})))
end)

it('isEmpty()', function()
  assertEquals(true, moses.isEmpty(''))
  assertEquals(true, moses.isEmpty({}))
  assertEquals(false, moses.isEmpty({'a','b','c'}))
end)

it('isEqual()', function()
  assertEquals(true, moses.isEqual(1,1))
  assertEquals(false, moses.isEqual(true,false))
  assertEquals(false, moses.isEqual(3.14,math.pi))
  assertEquals(false, moses.isEqual({3,4,5},{3,4,{5}}))
end)

it('isFinite()', function()
  assertEquals(true, moses.isFinite(99e99))
  assertEquals(true, moses.isFinite(math.pi))
  assertEquals(false, moses.isFinite(math.huge))
  assertEquals(false, moses.isFinite(1/0))
  assertEquals(false, moses.isFinite(0/0))
end)

it('isFunction()', function()
  assertEquals(true, moses.isFunction(print))
  assertEquals(true, moses.isFunction(function() end))
  assertEquals(false, moses.isFunction({}))
end)

it('isInteger()', function()
  assertEquals(false, moses.isInteger(math.pi))
  assertEquals(true, moses.isInteger(1))
  assertEquals(true, moses.isInteger(-1))
end)

it('isNil()', function()
  assertEquals(true, moses.isNil(nil))
  assertEquals(true, moses.isNil())
  assertEquals(false, moses.isNil({}))
end)

it('isNumber()', function()
  assertEquals(true, moses.isNumber(math.pi))
  assertEquals(true, moses.isNumber(math.huge))
  assertEquals(true, moses.isNumber(0/0))
  assertEquals(false, moses.isNumber())
end)

it('isString()', function()
  assertEquals(true, moses.isString(''))
  assertEquals(true, moses.isString('hello'))
  assertEquals(false, moses.isString({}))
end)

it('isTable()', function()
  assertEquals(true, moses.isTable({}))
  assertEquals(true, moses.isTable(math))
  assertEquals(true, moses.isTable(string))
end)

it('keys(obj)', function()
  local res = moses.keys({1,2,3})
  assertContains(res, 1)
  assertContains(res, 2)
  assertContains(res, 3)
  
  res = moses.keys({x=0, y=1})
  assertContains(res, 'x')
  assertContains(res, 'y')
end)

it('map(array)', function()
  local res = moses.map({1,2,3}, function(v)
    return v+10
  end)
  assertEquals(3, #res)
  assertEquals(11, res[1])
  assertEquals(12, res[2])
  assertEquals(13, res[3])
end)

it('map(table)', function()
  local res = moses.map({a=1, b=2}, function(v, k)
    return k..v
  end)
  assertEquals('a1', res['a'])
  assertEquals('b2', res['b'])
  
  res = moses.map({a=1, b=2}, function(v, k)
    return k..k, v*2
  end)
  assertEquals(2, res['aa'])
  assertEquals(4, res['bb'])
end)

it('max(t)', function()
  assertEquals(3, moses.max({1,2,3}))
end)

it('max(t, transform)', function()
  local peoples = {
    {name='John',age=23}, {name='Peter',age=17},
    {name='Steve',age=15}, {age=33}
  }
  assertEquals(33, moses.max(peoples, function(people) return people.age end))
end)

it('min(t)', function()
  assertEquals(1, moses.min({1,2,3}))
end)

it('min(t, transform)', function()
  local peoples = {
    {name='John',age=23}, {name='Peter',age=17},
    {name='Steve',age=15}, {age=33}
  }
  assertEquals(15, moses.min(peoples, function(people) return people.age end))
end)

it('noop()', function()
  assertEquals(nil, moses.noop())
  assertEquals(nil, moses.noop(7))
end)

it('ones(n)', function()
  assertSame({1,1,1}, moses.ones(3))
end)

it('pack(...)', function()
  assertSame({1,2,8,'d','a',0}, moses.pack(1,2,8,'d','a',0))
end)

it('partition(array, n=2)', function()
  local t = {1,2,3,4,5,6}
  local iter = moses.partition(t,2)
  local res1 = iter()
  assertEquals(true, res1 ~= nil)
  assertEquals(1, res1[1])
  assertEquals(2, res1[2])
  local res2 = iter()
  assertEquals(true, res2 ~= nil)
  assertEquals(3, res2[1])
  assertEquals(4, res2[2])
  local res3 = iter()
  assertEquals(true, res3 ~= nil)
  assertEquals(5, res3[1])
  assertEquals(6, res3[2])
  local res4 = iter()
  assertEquals(nil, res4)
end)

it('partition(array, n=4)', function()
  local t = {1,2,3,4,5,6}
  local iter = moses.partition(t,4)
  local res1 = iter()
  assertEquals(true, res1 ~= nil)
  assertEquals(1, res1[1])
  assertEquals(2, res1[2])
  assertEquals(3, res1[3])
  assertEquals(4, res1[4])
  local res2 = iter()
  assertEquals(true, res2 ~= nil)
  assertEquals(5, res2[1])
  assertEquals(6, res2[2])
  local res3 = iter()
  assertEquals(nil, res3)
end)

it('pluck)', function()
  local peoples = {
    {name='John', age=23},{name='Peter', age=17},
    {name='Steve', age=15},{age=33}}
  
  assertSame({23,17,15,33}, moses.pluck(peoples,'age'))
  assertSame({'John','Peter','Steve'}, moses.pluck(peoples,'name'))
end)

it('range(from)', function()
  local res = moses.range(3)
  assertEquals(3, #res)
  assertEquals(1, res[1])
  assertEquals(2, res[2])
  assertEquals(3, res[3])
  
  res = moses.range(-5)
  assertEquals(5, #res)
  assertEquals(-1, res[1])
  assertEquals(-2, res[2])
  assertEquals(-3, res[3])
  assertEquals(-4, res[4])
  assertEquals(-5, res[5])
end)
  
it('range(from, to)', function()
  local res = moses.range(1,4)
  assertEquals(4, #res)
  assertEquals(1, res[1])
  assertEquals(2, res[2])
  assertEquals(3, res[3])
  assertEquals(4, res[4])
  
  res = moses.range(5,1)
  assertEquals(5, #res)
  assertEquals(5, res[1])
  assertEquals(4, res[2])
  assertEquals(3, res[3])
  assertEquals(2, res[4])
  assertEquals(1, res[5])
end)

it('range(from, to, step)', function()
  local res = moses.range(0, 2, 0.7)
  assertEquals(3, #res)
  assertEquals(0, res[1])
  assertEquals(0.7, res[2])
  assertEquals(1.4, res[3])
end)

it('reduce(t, f)', function()
  local function add(a,b) return a+b end
  local res = moses.reduce({1,2,3,4}, add)
  assertEquals(10, res)
  
  local function concat(a,b) return a..b end
  res = moses.reduce({'a','b','c','d'}, concat)
  assertEquals('abcd', res)
end)

it('reduce(t, f, state)', function()
  local function add(a,b) return a+b end
  local res = moses.reduce({1,2,3,4}, add, 100)
  assertEquals(110, res)
end)

it('rep()', function()
  assertSame({4,4,4}, moses.rep(4,3))
end)

it('result()', function()
  assertEquals(3, moses.result('abc', 'len'))
  assertEquals('abc', moses.result({'a','b','c'}, table.concat))
end)

it('reverse()', function()
  assertSame({'d',3,2,1}, moses.reverse({1,2,3,'d'}))
end)

it('same()', function()
  local a = {'a','b','c','d'}      
  local b = {'b','a','d','c'}
  assertEquals(true, moses.same(a,b))
  
  b[#b+1] = 'e'
  assertEquals(false, moses.same(a,b))
end)

it('select()', function()
  local function isEven(v) return v%2==0 end
  local function isOdd(v) return v%2~=0 end
  
  local evens = moses.select({1,2,3,4,5,6,7}, isEven)
  assertEquals(3, #evens)
  assertEquals(2, evens[1])
  assertEquals(4, evens[2])
  assertEquals(6, evens[3])
  
  local odds = moses.select({1,2,3,4,5,6,7}, isOdd)
  assertEquals(4, #odds)
  assertEquals(1, odds[1])
  assertEquals(3, odds[2])
  assertEquals(5, odds[3])
  assertEquals(7, odds[4])
end)

it('size()', function()
  assertEquals(3, moses.size({1,2,3}))
  assertEquals(2, moses.size({one=1, two=2}))
end)

it('slice()', function()
  local array = {1,2,3,4,5,6,7,8,9}
  local res = moses.slice(array, 3,6)
  assertEquals(4, #res)
  assertEquals(3, res[1])
  assertEquals(4, res[2])
  assertEquals(5, res[3])
  assertEquals(6, res[4])
end)

it('sort()', function()
  assertSame({'a','b','c','d'}, moses.sort({'b','a','d','c'}))
end)

it('sort() with a custom comparison function', function()
  local res = moses.sort({'b','a','d','c'}, function(a,b) return a:byte() > b:byte() end)
  assertSame({'d','c','b','a'}, res)
end)

it('sortBy() with transform function', function()
  local r = moses.sortBy({1,2,3,4,5}, math.sin)
  assertSame({5,4,3,1,2}, r)
end)

it('sortBy() with name property', function()
  local people = {
    {name='albert', age=40},
    {name='louis', age=55},
    {name='steve', age=35},
    {name='henry', age=19}
  }
  local r = moses.sortBy(people, 'age')
  assertSame({name='henry', age=19}, r[1])
  assertSame({name='steve', age=35}, r[2])
  assertSame({name='albert', age=40}, r[3])
  assertSame({name='louis', age=55}, r[4])
end)

it('sortBy() with name property and custom comparator', function()
  local people = {
    {name='albert', age=40},
    {name='louis', age=55},
    {name='steve', age=35},
    {name='henry', age=19}
  }
  local r = moses.sortBy(people, 'age', function(a,b) return a>b end)
  assertSame({name='louis', age=55}, r[1])
  assertSame({name='albert', age=40}, r[2])
  assertSame({name='steve', age=35}, r[3])
  assertSame({name='henry', age=19}, r[4])
end)

it('sortBy() defaults to identity', function()
  assertSame({1,2,3,4,5}, moses.sortBy({1,2,3,4,5}))
end)

it('tabulate()', function()
  local text = 'letters'
  local chars = string.gmatch(text, '.')
  local result = moses.tabulate(chars)
  assertEquals(7, #result)
  assertEquals('l', result[1])
  assertEquals('e', result[2])
  assertEquals('t', result[3])
  assertEquals('t', result[4])
  assertEquals('e', result[5])
  assertEquals('r', result[6])
  assertEquals('s', result[7])
end)

it('unique()', function()
  local res = moses.unique({1,1,2,2,3,3,4,4,4,5})
  assertEquals(5, #res)
  assertEquals(1, res[1])
  assertEquals(2, res[2])
  assertEquals(3, res[3])
  assertEquals(4, res[4])
  assertEquals(5, res[5])
end)

it('values(obj)', function()
  local res = moses.values({1,2,3})
  assertContains(res, 1)
  assertContains(res, 2)
  assertContains(res, 3)
  
  res = moses.values({x=0, y=1})
  assertContains(res, 0)
  assertContains(res, 1)
end)

it('zeros(n)', function()
  assertSame({0,0,0,0}, moses.zeros(4))
end)

it('zip(...)', function()
  local names = {'Bob','Alice','James'}
  local ages = {22,23}
  local res = moses.zip(names,ages)
  assertContains(res, {'Bob',22})
  assertContains(res, {'Alice',23})
  assertContains(res, {'James'})
end)

-- ============================================================================
-- Mini test framework -- report results
-- ============================================================================

print(string.format('End of test: %d failures', failures))
