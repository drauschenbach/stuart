local moses = require 'moses'
local socketUrl = require 'socket.url'

local LocalFileSystem = require 'stuart.LocalFileSystem'
local WebHdfsFileSystem = require 'stuart.WebHdfsFileSystem'

local FileSystemFactory = {}

-- ============================================================================
-- createForOpenPath(path)
-- ============================================================================
FileSystemFactory.createForOpenPath = function(path)

  local parsedUri = socketUrl.parse(path)
  
  -- --------------------------------------------------------------------------
  -- URI support
  -- --------------------------------------------------------------------------
  
  if parsedUri.scheme == 'webhdfs' or parsedUri.scheme == 'swebhdfs' then
    local constructorUri, openPath
    local uriSegments = moses.clone(parsedUri)
    local segments = socketUrl.parse_path(uriSegments.path)
    segments['is_absolute'] = nil
    if #segments > 2 and segments[1] == 'webhdfs' and segments[2] == 'v1' then
      -- split /webhdfs/v1/path/file into constructorUri=/webhdfs/v1/ and openPath=path/file 
      constructorUri = uriSegments.scheme .. '://' .. uriSegments.authority .. '/' .. segments[1] .. '/' .. segments[2] .. '/'
      openPath = table.concat(moses.rest(segments, 3), '/')
    elseif #segments > 1 and segments[1] == 'v1' then
      -- split /v1/path/file into constructorUri=/v1/ and openPath=path/file 
      constructorUri = uriSegments.scheme .. '://' .. uriSegments.authority .. '/' .. segments[1] .. '/'
      openPath = table.concat(moses.rest(segments, 2), '/')
    end
    local fs = WebHdfsFileSystem:new(constructorUri)
    return fs, openPath
  elseif parsedUri.scheme ~= nil then
    error('Unsupported URI scheme: ' .. parsedUri.scheme)
  end
  
  -- --------------------------------------------------------------------------
  -- local path support 
  -- --------------------------------------------------------------------------
  
  local segments = socketUrl.parse_path(path)
  segments['is_absolute'] = nil
  local constructorUri = table.concat(moses.first(segments, #segments - 1), '/') .. '/'
  local openPath = segments[#segments]
  local fs = LocalFileSystem:new(constructorUri)
  return fs, openPath
  
end

return FileSystemFactory
