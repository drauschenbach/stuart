local class = require 'stuart.class'
local FileSystem = require 'stuart.FileSystem'

local WebHdfsFileSystem = class.new(FileSystem)

function WebHdfsFileSystem:_init(uri)
  local has_luasocketHttp, _ = pcall(require, 'socket.http')
  assert(has_luasocketHttp)
  FileSystem._init(self, uri)
  local has_luasocketUrl, socketUrl = pcall(require, 'socket.url')
  assert(has_luasocketUrl)
  self.parsedUri = socketUrl.parse(uri)
end

function WebHdfsFileSystem:getFileStatus(path)
  local moses = require 'moses'
  local urlSegments = moses.clone(self.parsedUri)
  urlSegments.path = urlSegments.path .. '/v1/' .. (path or '')
  urlSegments.query = 'op=GETFILESTATUS'
  local socketUrl = require 'socket.url'
  local uri = socketUrl.build(urlSegments)
  local http = require 'socket.http'
  local json, status, headers = http.request(uri)
  local jsonDecode = require 'stuart.util'.jsonDecode
  local obj = jsonDecode(json)
  if obj.RemoteException then error(obj.RemoteException.message) end
  return obj.FileStatus, status, headers
end

function WebHdfsFileSystem:isDirectory(path)
  local fileStatus = self:getFileStatus(path)
  return fileStatus and fileStatus.type == 'DIRECTORY'
end

function WebHdfsFileSystem:listStatus(path)
  local moses = require 'moses'
  local urlSegments = moses.clone(self.parsedUri)
  urlSegments.path = urlSegments.path .. '/v1/' .. (path or '')
  urlSegments.query = 'op=LISTSTATUS'
  local socketUrl = require 'socket.url'
  local uri = socketUrl.build(urlSegments)
  local http = require 'socket.http'
  local json, status, headers = http.request(uri)
  local jsonDecode = require 'stuart.util'.jsonDecode
  local obj = jsonDecode(json)
  if obj.RemoteException then error(obj.RemoteException.message) end
  return obj.FileStatuses.FileStatus, status, headers
end

function WebHdfsFileSystem:mkdirs(path)
  local moses = require 'moses'
  local urlSegments = moses.clone(self.parsedUri)
  urlSegments.path = urlSegments.path .. '/v1/' .. (path or '')
  urlSegments.query = 'op=MKDIRS'
  local socketUrl = require 'socket.url'
  local uri = socketUrl.build(urlSegments)
  local http = require 'socket.http'
  local json, status, headers = http.request(uri)
  local jsonDecode = require 'stuart.util'.jsonDecode
  local obj = jsonDecode(json)
  if obj.RemoteException then error(obj.RemoteException.message) end
  return obj.boolean, status, headers
end

function WebHdfsFileSystem:open(path)
  local moses = require 'moses'
  local urlSegments = moses.clone(self.parsedUri)
  urlSegments.path = urlSegments.path .. '/v1/' .. (path or '')
  urlSegments.query = 'op=OPEN'
  local socketUrl = require 'socket.url'
  local uri = socketUrl.build(urlSegments)
  local http = require 'socket.http'
  local data, status, headers = http.request(uri)
  return data, status, headers
end

return WebHdfsFileSystem
