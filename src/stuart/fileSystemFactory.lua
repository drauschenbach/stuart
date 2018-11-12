local M = {}

-- ============================================================================
-- createForOpenPath(path)
-- ============================================================================
M.createForOpenPath = function(path)
  local moses = require 'moses'
  local urlParse = require 'stuart.util'.urlParse
  
  local parsedUri = urlParse(path)
  
  -- --------------------------------------------------------------------------
  -- URI support
  -- --------------------------------------------------------------------------
  
  local split = require 'stuart.util'.split
  local segments = split(parsedUri.path, '/')
  if parsedUri.scheme == 'webhdfs' or parsedUri.scheme == 'swebhdfs' then
    local constructorUri, openPath
    local uriSegments = moses.clone(parsedUri)
    if #segments > 2 and segments[1] == 'webhdfs' and segments[2] == 'v1' then
      -- split /webhdfs/v1/path/file into constructorUri=/webhdfs/v1/ and openPath=path/file
      constructorUri = string.format('%s://%s/%s/%s/', uriSegments.scheme, uriSegments.host, segments[1], segments[2])
      openPath = table.concat(moses.rest(segments, 3), '/')
    elseif #segments > 1 and segments[1] == 'v1' then
      -- split /v1/path/file into constructorUri=/v1/ and openPath=path/file
      constructorUri = string.format('%s://%s/%s/', uriSegments.scheme, uriSegments.host, segments[1])
      openPath = table.concat(moses.rest(segments, 2), '/')
    else
      -- provide /webhdfs when absent
      constructorUri = string.format('%s://%s/webhdfs/', uriSegments.scheme, uriSegments.host)
      openPath = table.concat(segments, '/')
    end
    local WebHdfsFileSystem = require 'stuart.WebHdfsFileSystem'
    local fs = WebHdfsFileSystem.new(constructorUri)
    return fs, openPath
  elseif parsedUri.scheme ~= nil then
    error('Unsupported URI scheme: ' .. parsedUri.scheme)
  end
  
  -- --------------------------------------------------------------------------
  -- local path support
  -- --------------------------------------------------------------------------
  
  local constructorUri, openPath
  if #segments == 1 then
    constructorUri = './'
    openPath = segments[1]
  else
    constructorUri = table.concat(moses.first(segments, #segments - 1), '/') .. '/'
    openPath = segments[#segments]
  end
  local LocalFileSystem = require 'stuart.LocalFileSystem'
  local fs = LocalFileSystem.new(constructorUri)
  return fs, openPath
  
end

return M
