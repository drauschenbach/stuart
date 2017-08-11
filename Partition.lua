local _ = require 'lodash'

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
  return _.size(self.x)
end

function Partition:_flatten()
  self.x = _.flatten(self.x)
  return self
end

function Partition:_flattenValues()
  self.x = _.reduce(self.x, function(r, e)
    _.map(e[2], function(v)
      table.insert(r, {e[1], v})
    end)
    return r
  end, {})
  return self
end

return Partition
