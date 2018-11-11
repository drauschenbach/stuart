local class = require 'stuart.class'

-- Hadoop FileSystem adapter
local FileSystem = class.new()

function FileSystem:_init(uri)
  self.uri = uri
end

function FileSystem:getUri()
  return self.uri
end

return FileSystem
