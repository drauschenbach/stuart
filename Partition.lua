local moses = require 'moses'

Partition = {}

function Partition:new(o, x, index)
  o = o or {}
  o.x = x
  o.index = index
  setmetatable(o, self)
  self.__index = self
  return o
end

function Partition:_count()
  return #self.x
end

function Partition:_flatten()
  self.x = moses.flatten(self.x)
  return self
end

function Partition:_flattenValues()
  self.x = moses.reduce(self.x, function(r, e)
    local x = e[2]
    if moses.isString(x) then
      t = {}
      x:gsub('.', function(c) t[#t+1] = c end)
      x = t
    end
    moses.map(x, function(i, v)
      table.insert(r, {e[1], v})
    end)
    return r
  end, {})
  return self
end

function Partition:_toLocalIterator()
  local i = 0
  return function()
    i = i + 1
    if i <= #self.x then
      return self.x[i]
    end
  end
end

return Partition
