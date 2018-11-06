local class = require 'stuart.util.class'

-- Hadoop FileSystem adapter
local FileSystem = class.new('FileSystem')

function FileSystem:__init(uri)
  self.uri = uri
end

function FileSystem:getUri()
  return self.uri
end

return FileSystem
