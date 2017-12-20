--- Utility-belt library for functional programming in Lua ([source](http://github.com/Yonaba/Moses))
-- @author [Roland Yonaba](http://github.com/Yonaba)
-- @copyright 2012-2017
-- @license [MIT](http://www.opensource.org/licenses/mit-license.php)

-- Unused functions removed for Stuart

-- Internalisation
local next, type, pcall          = next, type, pcall
local setmetatable, getmetatable = setmetatable, getmetatable
local t_insert, t_sort           = table.insert, table.sort
local t_concat                   = table.concat
local randomseed, random, huge   = math.randomseed, math.random, math.huge
local floor, max, min            = math.floor, math.max, math.min
local unpack                     = table.unpack or unpack
local pairs,ipairs               = pairs,ipairs
local M                          = {}


-- ======== Private helpers

local function f_max(a,b) return a>b end
local function f_min(a,b) return a<b end
local function isTrue(_,value) return value and true end
local function iNot(value) return not value end

local function count(t)  -- raw count of items in an map-table
  local i = 0
    for k,v in pairs(t) do i = i + 1 end
  return i
end

local function extract(list,comp,transform,...) -- extracts value from a list
  local _ans
  transform = transform or M.identity
  for index,value in pairs(list) do
    if not _ans then _ans = transform(value,...)
    else
      local value2 = transform(value,...)
      _ans = comp(_ans,value2) and _ans or value2
    end
  end
  return _ans
end

local function partgen(t, n, f, pad) -- generates array partitions
  for i = 0, #t, n do
    local s = M.slice(t, i+1, i+n)
    if #s>0 then
      while (#s < n and pad) do s[#s+1] = pad end
      f(s)
    end
  end
end

-- Internal counter for unique ids generation
local unique_id_counter = -1

--- Table functions
-- @section Table functions

--- Clears a table. All its values become nil.
-- @name clear
-- @param t a table
-- @return the given table, cleared.
function M.clear(t)
  for k in pairs(t) do t[k] = nil end
  return t
end

--- Iterates on key-value pairs, calling `f (k, v)` at every step.
-- <br/><em>Aliased as `forEach`</em>.
-- @name each
-- @param t a table
-- @param f a function, prototyped as `f (k, v, ...)`
-- @param[opt] ... Optional args to be passed to `f`
-- @see eachi
function M.each(t, f, ...)
  for index,value in pairs(t) do
    f(index,value,...)
  end
end

--- Iterates on integer key-value pairs, calling `f(k, v)` every step.
-- Only applies to values located at integer keys. The table can be a sparse array.
-- Iteration will start from the lowest integer key found to the highest one.
-- <br/><em>Aliased as `forEachi`</em>.
-- @name eachi
-- @param t a table
-- @param f a function, prototyped as `f (k, v, ...)`
-- @param[opt] ... Optional args to be passed to `f`
-- @see each
function M.eachi(t, f, ...)
  local lkeys = M.sort(M.select(M.keys(t), function(_,v)
    return M.isInteger(v)
  end))
  for k, key in ipairs(lkeys) do
    f(key, t[key],...)
  end
end

--- Counts occurrences of a given value in a table. Uses @{isEqual} to compare values.
-- @name count
-- @param t a table
-- @param[opt] value a value to be searched in the table. If not given, the @{size} of the table will be returned
-- @return the count of occurrences of the given value
-- @see countf
-- @see size
function M.count(t, value)
  if M.isNil(value) then return M.size(t) end
  local r = 0
  M.each(t, function(_,v)
    if M.isEqual(v, value) then r = r + 1 end
  end)
  return r
end

--- Maps `f (k, v)` on key-value pairs, collects and returns the results.
-- <br/><em>Aliased as `collect`</em>.
-- @name map
-- @param t a table
-- @param f  an iterator function, prototyped as `f (k, v, ...)`
-- @param[opt] ... Optional args to be passed to `f`
-- @return a table of results
function M.map(t, f, ...)
  local _t = {}
  for index,value in pairs(t) do
    local k, kv, v = index, f(index,value,...)
    _t[v and kv or k] = v or kv
  end
  return _t
end

--- Reduces a table, left-to-right. Folds the table from the first element to the last element
-- to a single value, using a given iterator and an initial state.
-- The iterator takes a state and a value and returns a new state.
-- <br/><em>Aliased as `inject`, `foldl`</em>.
-- @name reduce
-- @param t a table
-- @param f an iterator function, prototyped as `f (state, value)`
-- @param[opt] state an initial state of reduction. Defaults to the first value in the table.
-- @return the final state of reduction
-- @see reduceRight
-- @see reduceby
function M.reduce(t, f, state)
  for __,value in pairs(t) do
    if state == nil then state = value
    else state = f(state,value)
    end
  end
  return state
end

--- Reduces values in a table passing a given predicate. Folds the table left-to-right, considering
-- only values validating a given predicate.
-- @name reduceby
-- @param t a table
-- @param f an iterator function, prototyped as `f (state, value)`
-- @param state an initial state of reduction.
-- @param pred a predicate function `pred (k, v, ...)` to select values to be considered for reduction
-- @param[opt] ... optional args to be passed to `pred`
-- @return the final state of reduction
-- @see reduce
function M.reduceby(t, f, state, pred, ...)
  return M.reduce(M.select(t, pred, ...), f, state)
end

--- Reduces a table, right-to-left. Folds the table from the last element to the first element
-- to single value, using a given iterator and an initial state.
-- The iterator takes a state and a value, and returns a new state.
-- <br/><em>Aliased as `injectr`, `foldr`</em>.
-- @name reduceRight
-- @param t a table
-- @param f an iterator function, prototyped as `f (state, value)`
-- @param[opt] state an initial state of reduction. Defaults to the last value in the table.
-- @return the final state of reduction
-- @see reduce
function M.reduceRight(t, f, state)
  return M.reduce(M.reverse(t),f,state)
end

--- Reduces a table while saving intermediate states. Folds the table left-to-right
-- using a given iterator and an initial state. The iterator takes a state and a value,
-- and returns a new state. The result is an array of intermediate states.
-- <br/><em>Aliased as `mapr`</em>
-- @name mapReduce
-- @param t a table
-- @param f an iterator function, prototyped as `f (state, value)`
-- @param[opt] state an initial state of reduction. Defaults to the first value in the table.
-- @return an array of states
-- @see mapReduceRight
function M.mapReduce(t, f, state)
  local _t = {}
  for i,value in pairs(t) do
    _t[i] = not state and value or f(state,value)
    state = _t[i]
  end
  return _t
end

--- Reduces a table while saving intermediate states. Folds the table right-to-left
-- using a given iterator and an initial state. The iterator takes a state and a value,
-- and returns a new state. The result is an array of intermediate states.
-- <br/><em>Aliased as `maprr`</em>
-- @name mapReduceRight
-- @param t a table
-- @param f an iterator function, prototyped as `f (state, value)`
-- @param[opt] state an initial state of reduction. Defaults to the last value in the table.
-- @return an array of states
-- @see mapReduce
function M.mapReduceRight(t, f, state)
  return M.mapReduce(M.reverse(t),f,state)
end

--- Performs a linear search for a value in a table. It does not work for nested tables.
-- The given value can be a function prototyped as `f (v, value)` which should return true when
-- any v in the table equals the value being searched.
-- <br/><em>Aliased as `any`, `some`, `contains`</em>
-- @name include
-- @param t a table
-- @param value a value to search for
-- @return a boolean : `true` when found, `false` otherwise
-- @see detect
function M.include(t, value)
  local _iter = M.isFunction(value) and value or M.isEqual
  for __,v in pairs(t) do
    if _iter(v,value) then return true end
  end
  return false
end

--- Performs a linear search for a value in a table. Returns the key of the value if found.
-- The given value can be a function prototyped as `f (v, value)` which should return true when
-- any v in the table equals the value being searched.
-- @name detect
-- @param t a table
-- @param value a value to search for
-- @return the key of the value when found or __nil__
-- @see include
function M.detect(t, value)
  local _iter = M.isFunction(value) and value or M.isEqual
  for key,arg in pairs(t) do
    if _iter(arg,value) then return key end
  end
end

--- Returns all values having specified keys `props`.
-- @name where
-- @param t a table
-- @param props a set of keys
-- @return an array of values from the passed-in table
-- @see findWhere
function M.where(t, props)
  local r = M.select(t, function(_,v)
    for key in pairs(props) do
      if v[key] ~= props[key] then return false end
    end
    return true
  end)
  return #r > 0 and r or nil
end

--- Returns the first value having specified keys `props`.
-- @name findWhere
-- @param t a table
-- @param props a set of keys
-- @return a value from the passed-in table
-- @see where
function M.findWhere(t, props)
  local index = M.detect(t, function(v)
    for key in pairs(props) do
      if props[key] ~= v[key] then return false end
    end
    return true
  end)
  return index and t[index]
end

--- Selects and returns values passing an iterator test.
-- <br/><em>Aliased as `filter`</em>.
-- @name select
-- @param t a table
-- @param f an iterator function, prototyped as `f (k, v, ...)`
-- @param[opt] ... Optional args to be passed to `f`
-- @return the selected values
-- @see reject
function M.select(t, f, ...)
  local _t = {}
  for index,value in pairs(t) do
    if f(index, value,...) then _t[#_t+1] = value end
  end
  return _t
end

--- Clones a table while dropping values passing an iterator test.
-- <br/><em>Aliased as `discard`</em>
-- @name reject
-- @param t a table
-- @param f an iterator function, prototyped as `f (k, v, ...)`
-- @param[opt] ... Optional args to be passed to `f`
-- @return the remaining values
-- @see select
function M.reject(t, f, ...)
  local _mapped = M.map(t,f,...)
  local _t = {}
  for index,value in pairs (_mapped) do
    if not value then _t[#_t+1] = t[index] end
  end
  return _t
end

--- Checks if all values in a table are passing an iterator test.
-- <br/><em>Aliased as `every`</em>
-- @name all
-- @param t a table
-- @param f an iterator function, prototyped as `f (k, v, ...)`
-- @param[opt] ... Optional args to be passed to `f`
-- @return `true` if all values passes the predicate, `false` otherwise
function M.all(t, f, ...)
  return ((#M.select(M.map(t,f,...), isTrue)) == count(t))
end

--- Extracts values in a table having a given key.
-- @name pluck
-- @param t a table
-- @param key a key, will be used to index in each value: `value[key]`
-- @return an array of values having the given key
function M.pluck(t, key)
  return M.reject(M.map(t,function(_,value)
      return value[key]
    end), iNot)
end

--- Returns the max value in a collection. If an transformation function is passed, it will
-- be used to evaluate values by which all objects will be sorted.
-- @name max
-- @param t a table
-- @param[opt] transform a transformation function, prototyped as `transform (v, ...)`, defaults to @{identity}
-- @param[optchain] ... Optional args to be passed to `transform`
-- @return the max value found
-- @see min
function M.max(t, transform, ...)
  return extract(t, f_max, transform, ...)
end

--- Returns the min value in a collection. If an transformation function is passed, it will
-- be used to evaluate values by which all objects will be sorted.
-- @name min
-- @param t a table
-- @param[opt] transform a transformation function, prototyped as `transform (v, ...)`, defaults to @{identity}
-- @param[optchain] ... Optional args to be passed to `transform`
-- @return the min value found
-- @see max
function M.min(t, transform, ...)
  return extract(t, f_min, transform, ...)
end

--- Returns a shuffled copy of a given collection. If a seed is provided, it will
-- be used to init the pseudo random number generator (using `math.randomseed`).
-- @name shuffle
-- @param t a table
-- @param[opt] seed a seed
-- @return a shuffled copy of the given table
function M.shuffle(t, seed)
  if seed then randomseed(seed) end
  local _shuffled = {}
  M.each(t,function(index,value)
     local randPos = floor(random()*index)+1
    _shuffled[index] = _shuffled[randPos]
    _shuffled[randPos] = value
  end)
  return _shuffled
end

--- Checks if two tables are the same. It compares if both tables features the same values,
-- but not necessarily at the same keys.
-- @name same
-- @param a a table
-- @param b another table
-- @return `true` or `false`
function M.same(a, b)
  return M.all(a, function (_,v) return M.include(b,v) end)
     and M.all(b, function (_,v) return M.include(a,v) end)
end

--- Sorts a table, in-place. If a comparison function is given, it will be used to sort values.
-- @name sort
-- @param t a table
-- @param[opt] comp a comparison function prototyped as `comp (a, b)`, defaults to <tt><</tt> operator.
-- @return the initial table, sorted.
-- @see sortBy
function M.sort(t, comp)
  t_sort(t, comp)
  return t
end

--- Sorts a table in-place using a transform. Values are ranked in a custom order of the results of
-- running `transform (v)` on all values. `transform` may also be a string name property  sort by.
-- `comp` is a comparison function.
-- @name sortBy
-- @param t a table
-- @param[opt] transform a `transform` function to sort elements prototyped as `transform (v)`. Defaults to @{identity}
-- @param[optchain] comp a comparision function, defaults to the `<` operator
-- @return a new array of sorted values
-- @see sort
function M.sortBy(t, transform, comp)
  local f = transform or M.identity
  if M.isString(transform) then
    f = function(u) return u[transform] end
  end
  comp = comp or f_min
  local _t = {}
  M.each(t, function(_,v)
    _t[#_t+1] = {value = v, transform = f(v)}
  end)
  t_sort(_t, function(a,b) return comp(a.transform, b.transform) end)
  return M.pluck(_t, 'value')
end

--- Splits a table into subsets groups.
-- @name groupBy
-- @param t a table
-- @param iter an iterator function, prototyped as `iter (k, v, ...)`
-- @param[opt] ... Optional args to be passed to `iter`
-- @return a table of subsets groups
function M.groupBy(t, iter, ...)
  local vararg = {...}
  local _t = {}
  M.each(t, function(i,v)
      local _key = iter(i,v, unpack(vararg))
      if _t[_key] then _t[_key][#_t[_key]+1] = v
      else _t[_key] = {v}
      end
    end)
  return _t
end

--- Counts the number of values in a collection. If being passed more than one argument
-- it will return the count of all passed-in arguments.
-- @name size
-- @param[opt] ... Optional variable number of arguments
-- @return a count
-- @see count
-- @see countf
function M.size(...)
  local args = {...}
  local arg1 = args[1]
  if M.isTable(arg1) then
    return count(args[1])
  else
    return count(args)
  end
end

--- Array functions
-- @section Array functions

--- Samples `n` random values from an array. If `n` is not specified, returns a single element.
-- It uses internally @{shuffle} to shuffle the array before sampling values. If `seed` is passed,
-- it will be used for shuffling.
-- @name sample
-- @param array an array
-- @param[opt] n a number of elements to be sampled. Defaults to 1.
-- @param[optchain] seed an optional seed for shuffling
-- @return an array of selected values or a single value when `n` == 1
-- @see sampleProb
function M.sample(array, n, seed)
  n = n or 1
  if n < 1 then return end
  if n == 1 then
    if seed then randomseed(seed) end
    return array[random(1, #array)]
  end
  return M.slice(M.shuffle(array, seed), 1, n)
end

--- Converts a list of arguments to an array.
-- @name toArray
-- @param ... a list of arguments
-- @return an array of all passed-in args
function M.toArray(...) return {...} end

--- Looks for the first occurrence of a given value in an array. Returns the value index if found.
-- Uses @{isEqual} to compare values.
-- @name find
-- @param array an array of values
-- @param value a value to lookup for
-- @param[opt] from the index from where the search will start. Defaults to 1.
-- @return the index of the value if found in the array, `nil` otherwise.
function M.find(array, value, from)
  for i = from or 1, #array do
    if M.isEqual(array[i], value) then return i end
  end
end

--- Returns an array where values are in reverse order. The passed-in array should not be sparse.
-- @name reverse
-- @param array an array
-- @return a reversed array
function M.reverse(array)
  local _array = {}
  for i = #array,1,-1 do
    _array[#_array+1] = array[i]
  end
  return _array
end

--- Replaces elements in a given array with a given value. In case `i` and `j` are given
-- it will only replaces values at indexes between `[i,j]`. In case `j` is greather than the array
-- size, it will append new values, increasing the array.
-- @name fill
-- @param array an array
-- @param value a value
-- @param[opt] i the index from which to start replacing values. Defaults to 1.
-- @param[optchain] j the index where to stop replacing values. Defaults to the array size.
-- @return the original array with values changed
function M.fill(array, value, i, j)
  j = j or M.size(array)
  for k = i or 1, j do array[k] = value end
  return array
end

--- Returns the index at which a value should be inserted. This index is evaluated so
-- that it maintains the sort. If a comparison function is passed, it will be used to sort
-- values.
-- @name sortedIndex
-- @param array an array
-- @param the value to be inserted
-- @param[opt] comp an comparison function prototyped as `f (a, b)`, defaults to <tt><</tt> operator.
-- @param[optchain] sort whether or not the passed-in array should be sorted
-- @return number the index at which the passed-in value should be inserted
function M.sortedIndex(array, value, comp, sort)
  local _comp = comp or f_min
  if sort then M.sort(array,_comp) end
  for i = 1,#array do
    if not _comp(array[i],value) then return i end
  end
  return #array+1
end

--- Returns the index of the first occurence of value in an array.
-- @name indexOf
-- @param array an array
-- @param value the value to search for
-- @return the index of the passed-in value
-- @see lastIndexOf
function M.indexOf(array, value)
  for k = 1,#array do
    if array[k] == value then return k end
  end
end

--- Returns the index of the last occurrence of value in an array.
-- @name lastIndexOf
-- @param array an array
-- @param value the value to search for
-- @return the index of the last occurrence of the passed-in value or __nil__
-- @see indexOf
function M.lastIndexOf(array, value)
  local key = M.indexOf(M.reverse(array),value)
  if key then return #array-key+1 end
end

--- Returns the first index at which a predicate returns true.
-- @name findIndex
-- @param array an array
-- @param predicate a predicate function prototyped as `predicate (k, v, ...)`
-- @param[opt] ... optional arguments to `pred`
-- @return the index found or __nil__
-- @see findLastIndex
function M.findIndex(array, predicate, ...)
  for k = 1, #array do
    if predicate(k,array[k],...) then return k end
  end
end

--- Returns the last index at which a predicate returns true.
-- @name findLastIndex
-- @param array an array
-- @param predicate a predicate function prototyped as `predicate (k, v, ...)`
-- @param[opt] ... optional arguments to `pred`
-- @return the index found or __nil__
-- @see findIndex
function M.findLastIndex(array, predicate, ...)
  local key = M.findIndex(M.reverse(array),predicate,...)
  if key then return #array-key+1 end
end

--- Slices values indexed within `[start, finish]` range.
-- <br/><em>Aliased as `M.sub`</em>
-- @name slice
-- @param array an array
-- @param[opt] start the lower bound index, defaults to the first index in the array.
-- @param[optchain] finish the upper bound index, defaults to the array length.
-- @return a new array of sliced values
function M.slice(array, start, finish)
  return M.select(array, function(index)
      return (index >= (start or next(array)) and index <= (finish or #array))
    end)
end

--- Returns the first N values in an array.
-- <br/><em>Aliased as `head`, `take`</em>
-- @name first
-- @param array an array
-- @param[opt] n the number of values to be collected, defaults to 1.
-- @return a new array
-- @see initial
-- @see last
-- @see rest
function M.first(array, n)
  n = n or 1
  return M.slice(array,1, min(n,#array))
end

--- Returns all values in an array excluding the last N values.
-- @name initial
-- @param array an array
-- @param[opt] n the number of values to be left, defaults to the array length.
-- @return a new array
-- @see first
-- @see last
-- @see rest
function M.initial(array, n)
  if n and n < 0 then return end
  return M.slice(array,1, n and #array-(min(n,#array)) or #array-1)
end

--- Returns the last N values in an array.
-- @name last
-- @param array an array
-- @param[opt] n the number of values to be collected, defaults to the array length.
-- @return a new array
-- @see first
-- @see initial
-- @see rest
function M.last(array, n)
  if n and n <= 0 then return end
  return M.slice(array,n and #array-min(n-1,#array-1) or 2,#array)
end

--- Removes all values before index.
-- <br/><em>Aliased as `tail`</em>
-- @name rest
-- @param array an array
-- @param[opt] index an index, defaults to 1
-- @return a new array
-- @see first
-- @see initial
-- @see last
function M.rest(array,index)
  if index and index > #array then return {} end
  return M.slice(array,index and max(1,min(index,#array)) or 1,#array)
end

--- Removes all falsy (false and nil) values.
-- @name compact
-- @param array an array
-- @return a new array
function M.compact(array)
  return M.reject(array, function (_,value)
    return not value
  end)
end

--- Flattens a nested array. Passing `shallow` will only flatten at the first level.
-- @name flatten
-- @param array an array
-- @param[opt] shallow specifies the flattening depth
-- @return a new array, flattened
function M.flatten(array, shallow)
  shallow = shallow or false
  local new_flattened
  local _flat = {}
  for key,value in pairs(array) do
    if M.isTable(value) then
      new_flattened = shallow and value or M.flatten (value)
      M.each(new_flattened, function(_,item) _flat[#_flat+1] = item end)
    else _flat[#_flat+1] = value
    end
  end
  return _flat
end

--- Returns values from an array not present in all passed-in args.
-- <br/><em>Aliased as `without` and `diff`</em>
-- @name difference
-- @param array an array
-- @param another array
-- @return a new array
-- @see union
-- @see intersection
-- @see symmetricDifference
function M.difference(array, array2)
  if not array2 then return M.clone(array) end
  return M.select(array,function(_,value)
      return not M.include(array2,value)
    end)
end

--- Returns the duplicate-free union of all passed in arrays.
-- @name union
-- @param ... a variable number of arrays arguments
-- @return a new array
-- @see difference
-- @see intersection
-- @see symmetricDifference
function M.union(...)
  return M.uniq(M.flatten({...}))
end

--- Returns the  intersection of all passed-in arrays.
-- Each value in the result is present in each of the passed-in arrays.
-- @name intersection
-- @param array an array
-- @param ... a variable number of array arguments
-- @return a new array
-- @see difference
-- @see union
-- @see symmetricDifference
function M.intersection(array, ...)
  local arg = {...}
  local _intersect = {}
  for i,value in ipairs(array) do
    if M.all(arg,function(_,v)
          return M.include(v,value)
        end) then
      t_insert(_intersect,value)
    end
  end
  return _intersect
end

--- Produces a duplicate-free version of a given array.
-- <br/><em>Aliased as `uniq`</em>
-- @name unique
-- @param array an array
-- @return a new array, duplicate-free
-- @see isunique
function M.unique(array)
  local ret = {}
  for i = 1, #array do
    if not M.find(ret, array[i]) then
      ret[#ret+1] = array[i]
    end
  end
  return ret
end

--- Merges values of each of the passed-in arrays in subsets.
-- Only values indexed with the same key in the given arrays are merged in the same subset.
-- <br/><em>Aliased as `transpose`</em>
-- @name zip
-- @param ... a variable number of array arguments
-- @return a new array
function M.zip(...)
  local arg = {...}
  local _len = M.max(M.map(arg,function(_,v)
      return #v
    end))
  local _ans = {}
  for i = 1,_len do
    _ans[i] = M.pluck(arg,i)
  end
  return _ans
end

--- Clones `array` and appends `other` values.
-- @name append
-- @param array an array
-- @param other an array
-- @return a new array
function M.append(array, other)
  local t = {}
  for i,v in ipairs(array) do t[i] = v end
  for i,v in ipairs(other) do t[#t+1] = v end
  return t
end

--- Produces a flexible list of numbers. If one positive value is passed, will count from 0 to that value,
-- with a default step of 1. If two values are passed, will count from the first one to the second one, with the
-- same default step of 1. A third value passed will be considered a step value.
-- @name range
-- @param[opt] from the initial value of the range
-- @param[optchain] to the final value of the range
-- @param[optchain] step the step of count
-- @return a new array of numbers
function M.range(...)
  local arg = {...}
  local _start,_stop,_step
  if #arg==0 then return {}
  elseif #arg==1 then _stop,_start,_step = arg[1],0,1
  elseif #arg==2 then _start,_stop,_step = arg[1],arg[2],1
  elseif #arg == 3 then _start,_stop,_step = arg[1],arg[2],arg[3]
  end
  if (_step and _step==0) then return {} end
  
  -- BEGIN patch --------------------------------------------------------------
  if _start == 1 and _stop == 1 and _step == 1 then return {1} end
  -- END patch ----------------------------------------------------------------
  
  local _ranged = {}
  local _steps = max(floor((_stop-_start)/_step),0)
  for i=1,_steps do _ranged[#_ranged+1] = _start+_step*i end
  if #_ranged>0 then t_insert(_ranged,1,_start) end
  return _ranged
end

--- Creates an array list of `n` values, repeated.
-- @name rep
-- @param value a value to be repeated
-- @param n the number of repetitions of value.
-- @return a new array of `n` values
function M.rep(value, n)
  local ret = {}
  for i = 1, n do ret[#ret+1] = value end
  return ret
end

--- Iterator returning partitions of an array. It returns arrays of length `n`
-- made of values from the given array. If the last partition has lower elements than `n` and
-- `pad` is supplied, it will be adjusted to `n` of elements with `pad` value.
-- @name partition
-- @param array an array
-- @param[opt] n the size of partitions. Should be greater than 0. Defaults to 1.
-- @param[optchain] pad a value to adjust the last subsequence to the `n` elements
-- @return an iterator function
function M.partition(array, n, pad)
  if n<=0 then return end
  return coroutine.wrap(function()
    partgen(array, n or 1, coroutine.yield, pad)
  end)
end

--- Concatenates values in a given array. Handles booleans as well. If `sep` string is
-- passed, it will be used as a separator. Passing `i` and `j` will result in concatenating
-- only values within `[i, j]` range.
-- <br/><em>Aliased as `join`</em>
-- @name concat
-- @param array a given array
-- @param[opt] sep a separator string, defaults to the empty string `''`.
-- @param[optchain] i the starting index, defaults to 1.
-- @param[optchain] j the final index, defaults to the array length.
-- @return a string
function M.concat(array, sep, i, j)
  local _array = M.map(array,function(_,v)
    return tostring(v)
  end)
  return t_concat(_array,sep,i or 1,j or #array)

end

--- Utility functions
-- @section Utility functions

--- The no-operation function.
-- @name noop
-- @return nothing
function M.noop() return end

--- Returns the passed-in value. This function is used internally
-- as a default iterator.
-- @name identity
-- @param value a value
-- @return the passed-in value
function M.identity(value) return value end

--- Creates a constant function which returns the same output on every call.
-- @name constant
-- @param value a constant value
-- @return a constant function
function M.constant(value) return function() return value end end

--- Binds `v` to be the first argument to `f`. Calling `f (...)` will result to `f (v, ...)`.
-- @name bind
-- @param f a function
-- @param v a value
-- @return a function
-- @see bind2
-- @see bindn
-- @see bindAll
function M.bind(f, v)
  return function (...)
      return f(v,...)
    end
end

--- Binds `v` to be the second argument to `f`. Calling `f (a, ...)` will result to `f (a, v, ...)`.
-- @name bind2
-- @param f a function
-- @param v a value
-- @return a function
-- @see bind
-- @see bindn
-- @see bindAll
function M.bind2(f, v)
  return function (t, ...)
    return f(t, v, ...)
  end
end

--- Binds `...` to be the N-first arguments to function `f`.
-- Calling `f (a1, a2, ..., aN)` will result to `f (..., a1, a2, ...,aN)`.
-- @name bindn
-- @param f a function
-- @param ... a variable number of arguments
-- @return a function
-- @see bind
-- @see bind2
-- @see bindAll
function M.bindn(f, ...)
  local iArg = {...}
  return function (...)
      return f(unpack(M.append(iArg,{...})))
    end
end

--- Generates an unique ID for the current session. If given a string `template`, it
-- will use this template for output formatting. Otherwise, if `template` is a function, it
-- will evaluate `template (id, ...)`.
-- <br/><em>Aliased as `uid`</em>.
-- @name uniqueId
-- @param[opt] template either a string or a function template to format the ID
-- @param[optchain] ... a variable number of arguments to be passed to `template`, in case it is a function.
-- @return value an ID
function M.uniqueId(template, ...)
  unique_id_counter = unique_id_counter + 1
  if template then
    if M.isString(template) then
      return template:format(unique_id_counter)
    elseif M.isFunction(template) then
      return template(unique_id_counter,...)
    end
  end
  return unique_id_counter
end

--- Produces an iterator which repeatedly apply a function `f` onto an input.
-- Yields x, then f(x), then f(f(x)), continuously.
-- @name iterator
-- @param f a function
-- @param x an initial input to `f`
-- @return an iterator fnction
-- <br/><em>Aliased as `iter`</em>.
function M.iterator(f, x)
  return function()
    x = f(x)
    return x
  end
end

--- Iterates an iterator and returns its values in an array.
-- @name array
-- @param ... an iterator (a function, a table and a value)
-- @return an array of results
function M.array(...)
  local r = {}
  for v in ... do r[#r+1] = v end
  return r
end

--- Object functions
--@section Object functions

--- Returns the keys of the object properties.
-- @name keys
-- @param obj an object
-- @return an array
function M.keys(obj)
  local _oKeys = {}
  M.each(obj,function(key) _oKeys[#_oKeys+1]=key end)
  return _oKeys
end

--- Returns the values of the object properties.
-- @name values
-- @param obj an object
-- @return an array
function M.values(obj)
  local _oValues = {}
  M.each(obj,function(_,value) _oValues[#_oValues+1]=value end)
  return _oValues
end

--- Converts keys and values a an array-list of [k, v].
-- @name kvpairs
-- @param obj an object
-- @return an array list of key-values pairs
-- @see toObj
function M.kvpairs(obj)
  local t = {}
  M.each(obj, function(k,v) t[#t+1] = {k,v} end)
  return t
end

--- Converts an array list of `kvpairs` to an object. Keys are taken
-- from the 1rst column in the `kvpairs` sequence, associated with values in the 2nd
-- column
-- @name toObj
-- @param kvpairs an array-list of `kvpairs`
-- @return an object
-- @see kvpairs
function M.toObj(kvpairs)
  local obj = {}
  for __, v in ipairs(kvpairs) do
    obj[v[1]] = v[2]
  end
  return obj
end

--- Returns a function that will return the key property of any passed-in object.
-- @name property
-- @param key a key property name
-- @return a function which should accept an object as argument
-- @see propertyOf
function M.property(key)
  return function(obj) return obj[key] end
end

--- Returns a function which will return the value of an object property.
-- @name propertyOf
-- @param obj an object
-- @return a function which should accept a key property argument
-- @see property
function M.propertyOf(obj)
  return function(key) return obj[key] end
end

--- Converts any given value to a boolean
-- @name toBoolean
-- @param value a value. Can be of any type
-- @return `true` if value is true, `false` otherwise (false or nil).
function M.toBoolean(value)
  return not not value
end

--- Clones a given object properties. If `shallow` is passed will also clone nested array properties.
-- @name clone
-- @param obj an object
-- @param[opt] shallow whether or not nested array-properties should be cloned, defaults to false.
-- @return a copy of the passed-in object
function M.clone(obj, shallow)
  if not M.isTable(obj) then return obj end
  local _obj = {}
  M.each(obj,function(i,v)
    if M.isTable(v) then
      if not shallow then
        _obj[i] = M.clone(v,shallow)
      else _obj[i] = v
      end
    else
      _obj[i] = v
    end
  end)
  return _obj
end

--- Checks if a given object implements a property.
-- @name has
-- @param obj an object
-- @param key a key property to be checked
-- @return `true` or `false`
function M.has(obj, key)
  return obj[key]~=nil
end

--- Returns an object copy having white-listed properties.
-- <br/><em>Aliased as `choose`</em>.
-- @name pick
-- @param obj an object
-- @param ... a variable number of string keys
-- @return the filtered object
function M.pick(obj, ...)
  local whitelist = M.flatten {...}
  local _picked = {}
  M.each(whitelist,function(_,property)
      if not M.isNil(obj[property]) then
        _picked[property] = obj[property]
      end
    end)
  return _picked
end

--- Returns an object copy without black-listed properties.
-- <br/><em>Aliased as `drop`</em>.
-- @name omit
-- @param obj an object
-- @param ... a variable number of string keys
-- @return the filtered object
function M.omit(obj, ...)
  local blacklist = M.flatten {...}
  local _picked = {}
  M.each(obj,function(key,value)
      if not M.include(blacklist,key) then
        _picked[key] = value
      end
    end)
  return _picked
end

--- Performs a deep comparison test between two objects. Can compare strings, functions
-- (by reference), nil, booleans. Compares tables by reference or by values. If `useMt`
-- is passed, the equality operator `==` will be used if one of the given objects has a
-- metatable implementing `__eq`.
-- <br/><em>Aliased as `M.compare`</em>
-- @name isEqual
-- @param objA an object
-- @param objB another object
-- @param[opt] useMt whether or not `__eq` should be used, defaults to false.
-- @return `true` or `false`
function M.isEqual(objA, objB, useMt)
  local typeObjA = type(objA)
  local typeObjB = type(objB)

  if typeObjA~=typeObjB then return false end
  if typeObjA~='table' then return (objA==objB) end

  local mtA = getmetatable(objA)
  local mtB = getmetatable(objB)

  if useMt then
    if (mtA or mtB) and (mtA.__eq or mtB.__eq) then
      return mtA.__eq(objA, objB) or mtB.__eq(objB, objA) or (objA==objB)
    end
  end

  if M.size(objA)~=M.size(objB) then return false end

  for i,v1 in pairs(objA) do
    local v2 = objB[i]
    if M.isNil(v2) or not M.isEqual(v1,v2,useMt) then return false end
  end

  for i,v1 in pairs(objB) do
    local v2 = objA[i]
    if M.isNil(v2) then return false end
  end

  return true
end

--- Checks if the given arg is a table.
-- @name isTable
-- @param t a value to be tested
-- @return `true` or `false`
function M.isTable(t)
  return type(t) == 'table'
end

--- Checks if the given argument is callable. Assumes `obj` is callable if
-- it is either a function or a table having a metatable implementing `__call` metamethod.
-- @name isCallable
-- @param obj an object
-- @return `true` or `false`
function M.isCallable(obj)
  return (M.isFunction(obj) or
     (M.isTable(obj) and getmetatable(obj)
                   and getmetatable(obj).__call~=nil) or false)
end

--- Checks if the given argument is an array. Assumes `obj` is an array
-- if is a table with consecutive integer keys starting at 1.
-- @name isArray
-- @param obj an object
-- @return `true` or `false`
function M.isArray(obj)
  if not M.isTable(obj) then return false end
  -- Thanks @Wojak and @Enrique García Cota for suggesting this
  -- See : http://love2d.org/forums/viewtopic.php?f=3&t=77255&start=40#p163624
  local i = 0
  for __ in pairs(obj) do
     i = i + 1
     if M.isNil(obj[i]) then return false end
  end
  return true
end

--- Checks if the given object is iterable with `pairs` (or `ipairs`).
-- @name isIterable
-- @param obj an object
-- @return `true` if the object can be iterated with `pairs` (or `ipairs`), `false` otherwise
function M.isIterable(obj)
  return M.toBoolean((pcall(pairs, obj)))
end

--- Checks if the given pbject is empty. If `obj` is a string, will return `true`
-- if `#obj == 0`. Otherwise, if `obj` is a table, will return whether or not this table
-- is empty. If `obj` is `nil`, it will return true.
-- @name isEmpty
-- @param[opt] obj an object
-- @return `true` or `false`
function M.isEmpty(obj)
  if M.isNil(obj) then return true end
  if M.isString(obj) then return #obj==0 end
  if M.isTable(obj) then return next(obj)==nil end
  return true
end

--- Checks if the given argument is a string.
-- @name isString
-- @param obj an object
-- @return `true` or `false`
function M.isString(obj)
  return type(obj) == 'string'
end

--- Checks if the given argument is a function.
-- @name isFunction
-- @param obj an object
-- @return `true` or `false`
function M.isFunction(obj)
   return type(obj) == 'function'
end

--- Checks if the given argument is nil.
-- @name isNil
-- @param obj an object
-- @return `true` or `false`
function M.isNil(obj)
  return obj==nil
end

--- Checks if the given argument is a number.
-- @name isNumber
-- @param obj an object
-- @return `true` or `false`
-- @see isNaN
function M.isNumber(obj)
  return type(obj) == 'number'
end

--- Checks if the given argument is NaN (see [Not-A-Number](http://en.wikipedia.org/wiki/NaN)).
-- @name isNaN
-- @param obj an object
-- @return `true` or `false`
-- @see isNumber
function M.isNaN(obj)
  return M.isNumber(obj) and obj~=obj
end

--- Checks if the given argument is a finite number.
-- @name isFinite
-- @param obj an object
-- @return `true` or `false`
function M.isFinite(obj)
  if not M.isNumber(obj) then return false end
  return obj > -huge and obj < huge
end

--- Checks if the given argument is a boolean.
-- @name isBoolean
-- @param obj an object
-- @return `true` or `false`
function M.isBoolean(obj)
  return type(obj) == 'boolean'
end

--- Checks if the given argument is an integer.
-- @name isInteger
-- @param obj an object
-- @return `true` or `false`
function M.isInteger(obj)
  return M.isNumber(obj) and floor(obj)==obj
end

-- Aliases

do

  -- Table functions aliases
  M.forEach     = M.each
  M.forEachi    = M.eachi
  M.loop        = M.cycle
  M.collect     = M.map
  M.inject      = M.reduce
  M.foldl       = M.reduce
  M.injectr     = M.reduceRight
  M.foldr       = M.reduceRight
  M.mapr        = M.mapReduce
  M.maprr       = M.mapReduceRight
  M.any         = M.include
  M.some        = M.include
  M.contains    = M.include
  M.filter      = M.select
  M.discard     = M.reject
  M.every       = M.all
  
  -- Array functions aliases
  M.takeWhile   = M.selectWhile
  M.rejectWhile = M.dropWhile
  M.shift       = M.pop
  M.remove      = M.pull
  M.rmRange     = M.removeRange
  M.chop        = M.removeRange
  M.sub         = M.slice
  M.head        = M.first
  M.take        = M.first
  M.tail        = M.rest
  M.skip        = M.last
  M.without     = M.difference
  M.diff        = M.difference
  M.symdiff     = M.symmetricDifference
  M.xor         = M.symmetricDifference
  M.uniq        = M.unique
  M.isuniq      = M.isunique
  M.transpose   = M.zip
  M.part        = M.partition
  M.perm        = M.permutation
  M.mirror      = M.invert
  M.join        = M.concat
  
  -- Utility functions aliases
  M.cache       = M.memoize
  M.juxt        = M.juxtapose
  M.uid         = M.uniqueId
  M.iter        = M.iterator
  
  -- Object functions aliases
  M.methods     = M.functions
  M.choose      = M.pick
  M.drop        = M.omit
  M.defaults    = M.template
  M.compare     = M.isEqual
  
end

-- Setting chaining and building interface

do

  -- Wrapper to Moses
  local f = {}

  -- Will be returned upon requiring, indexes into the wrapper
  local __ = {}
  __.__index = f

  -- Wraps a value into an instance, and returns the wrapped object
  local function new(value)
    local i = {_value = value, _wrapped = true}
    return setmetatable(i, __)
  end

  setmetatable(__,{
    __call  = function(_,v) return new(v) end, -- Calls returns to instantiation
    __index = function(_,key) return f[key] end  -- Redirects to the wrapper
  })

  --- Returns a wrapped object. Calling library functions as methods on this object
  -- will continue to return wrapped objects until @{obj:value} is used. Can be aliased as `_(value)`.
  -- @class function
  -- @name chain
  -- @param value a value to be wrapped
  -- @return a wrapped object
  function __.chain(value)
    return new(value)
  end

  --- Extracts the value of a wrapped object. Must be called on an chained object (see @{chain}).
  -- @class function
  -- @name obj:value
  -- @return the value previously wrapped
  function __:value()
    return self._value
  end

  -- Register chaining methods into the wrapper
  f.chain, f.value = __.chain, __.value

  -- Register all functions into the wrapper
  for fname,fct in pairs(M) do
    f[fname] = function(v, ...)
      local wrapped = M.isTable(v) and v._wrapped or false
      if wrapped then
        local _arg = v._value
        local _rslt = fct(_arg,...)
        return new(_rslt)
      else
        return fct(v,...)
      end
    end
  end

  return __
  
end
