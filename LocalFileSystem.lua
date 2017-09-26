local class = require 'middleclass'
local FileSystem = require 'FileSystem'

local LocalFileSystem = class('LocalFileSystem', FileSystem)

function LocalFileSystem:initialize(uri)
  FileSystem.initialize(self, uri)
end

function LocalFileSystem:open(path)
  local f = assert(io.open(path, 'r'))
  local data = f:read '*all'
  return data
end

return LocalFileSystem
