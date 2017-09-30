local class = require 'middleclass'
local FileSystem = require 'stuart.FileSystem'

local LocalFileSystem = class('LocalFileSystem', FileSystem)

function LocalFileSystem:initialize(uri)
  FileSystem.initialize(self, uri)
end

function LocalFileSystem:open(path)
  local f = assert(io.open(self.uri .. path, 'r'))
  local data = f:read '*all'
  return data
end

return LocalFileSystem
