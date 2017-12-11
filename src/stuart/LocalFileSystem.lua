local class = require 'middleclass'
local FileSystem = require 'stuart.FileSystem'
local has_lfs, lfs = pcall(require, 'lfs')

local LocalFileSystem = class('LocalFileSystem', FileSystem)

function LocalFileSystem:initialize(uri)
  FileSystem.initialize(self, uri)
end

function LocalFileSystem:isDirectory(path)

  if has_lfs then
    local attr, err = lfs.attributes(self.uri .. (path or ''))
    if err then error(err) end
    return attr.mode == 'directory'
  end
  
  local f = io.open(self.uri .. (path or ''), 'r')
  local isDir = not f:read(0) and f:seek('end') ~= 0
  f:close()
  return isDir
end

function LocalFileSystem:listStatus(path)

  if has_lfs then
    local fileStatuses = {}
    for file in lfs.dir(self.uri .. (path or '')) do
      local attr = lfs.attributes(file)
      if attr then
        fileStatuses[#fileStatuses+1] = {
          isDirectory= attr.mode == 'directory',
          isFile= attr.mode == 'file',
          isSymlink= attr.mode == 'link',
          len= attr.size,
          path= file
        }
      else -- https://github.com/keplerproject/luafilesystem/issues/104
        fileStatuses[#fileStatuses+1] = {
          isDirectory = false,
          isFile = true,
          isSymlink = false,
          path= file
        }
      end
    end
    return fileStatuses
  end
  
  error('list directory capability not present')
end

function LocalFileSystem:open(path)
  local f = assert(io.open(self.uri .. path, 'r'))
  local data = f:read '*all'
  return data
end

return LocalFileSystem
