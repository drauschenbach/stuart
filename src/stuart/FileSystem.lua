local class = require 'middleclass'

-- Hadoop FileSystem adapter
local FileSystem = class('FileSystem')

function FileSystem:initialize(uri)
  self.uri = uri
end

function FileSystem:getUri()
  return self.uri
end

return FileSystem
