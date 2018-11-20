local M = {}

M._createForLocalOpenPath = function(parsedUri)
  local moses = require 'moses'
  local split = require 'stuart.util'.split
  local segments = split(parsedUri.path, '/')
  local constructorUri, openPath
  if #segments == 1 or (#segments == 2 and segments[2] == '') then
    constructorUri = '.'
    openPath = segments[1]
  else
    constructorUri = table.concat(moses.first(segments, #segments - 1), '/')
    openPath = segments[#segments]
  end
  
  local LocalFileSystem = require 'stuart.LocalFileSystem'
  local fs = LocalFileSystem.new(constructorUri)
  return fs, openPath
end

M._createForWebHdfsOpenPath = function(parsedUri)
  local moses = require 'moses'
  local split = require 'stuart.util'.split
  local segments = split(parsedUri.path, '/')
  local constructorUri, openPath
  local uriSegments = moses.clone(parsedUri)
  if #segments > 3 and segments[2] == 'webhdfs' and segments[3] == 'v1' then
    -- split /webhdfs/v1/path/file into constructorUri=/webhdfs/v1/ and openPath=path/file
    constructorUri = string.format('%s://%s/%s/%s', uriSegments.scheme, uriSegments.host, segments[2], segments[3])
    openPath = '/' .. table.concat(moses.rest(segments, 4), '/')
  elseif #segments > 2 and segments[2] == 'v1' then
    -- split /v1/path/file into constructorUri=/v1/ and openPath=path/file
    constructorUri = string.format('%s://%s/%s', uriSegments.scheme, uriSegments.host, segments[2])
    openPath = '/' .. table.concat(moses.rest(segments, 2), '/')
  else
    -- provide /webhdfs when absent
    constructorUri = string.format('%s://%s/webhdfs', uriSegments.scheme, uriSegments.host)
    openPath = table.concat(segments, '/')
  end
  
  local WebHdfsFileSystem = require 'stuart.WebHdfsFileSystem'
  local fs = WebHdfsFileSystem.new(constructorUri)
  return fs, openPath
end

M.createForOpenPath = function(path)
  local urlParse = require 'stuart.util'.urlParse
  local parsedUri = urlParse(path)
  if parsedUri.scheme == 'webhdfs' or parsedUri.scheme == 'swebhdfs' then
    return M._createForWebHdfsOpenPath(parsedUri)
  elseif parsedUri.scheme ~= nil then
    error('Unsupported URI scheme: ' .. parsedUri.scheme)
  else
    return M._createForLocalOpenPath(parsedUri)
  end
end

return M
