local class = require 'stuart.class'
local FileSystem = require 'stuart.FileSystem'

local LocalFileSystem = class.new(FileSystem)

function LocalFileSystem:_init(uri)
  FileSystem._init(self, uri)
end

function LocalFileSystem:isDirectory(path)
  local has_lfs, lfs = pcall(require, 'lfs')
  if has_lfs then
    local attr, err = lfs.attributes(self.uri .. '/' .. (path or ''))
    if err then error(err) end
    return attr.mode == 'directory'
  end
  
  local f = io.open(self.uri .. (path or ''), 'r')
  local isDir = not f:read(0) and f:seek('end') ~= 0
  f:close()
  return isDir
end

function LocalFileSystem:listStatus(path)
  local has_lfs, lfs = pcall(require, 'lfs')
  if has_lfs then
    local fileStatuses = {}
    for file in lfs.dir(self.uri .. '/' .. (path or '')) do
      local attr, err = lfs.attributes(self.uri .. '/' .. (path or '') .. '/' .. file)
      if err then error(err) end
      fileStatuses[#fileStatuses+1] = {
        type= attr.mode:upper(),
        length= attr.size,
        pathSuffix= file
      }
    end
    return fileStatuses
  end
  
  error('list directory capability not present')
end

function LocalFileSystem:open(path)
  local f = assert(io.open(self.uri .. '/' .. path, 'r'))
  local data = f:read '*all'
  return data
end

return LocalFileSystem
