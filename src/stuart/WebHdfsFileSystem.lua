local class = require 'middleclass'
local http = require 'socket.http'
local jsonutil = require 'stuart.util.json'
local moses = require 'moses'
local url = require 'socket.url'

local FileSystem = require 'stuart.FileSystem'

local WebHdfsFileSystem = class('WebHdfsFileSystem', FileSystem)

function WebHdfsFileSystem:initialize(uri)
  FileSystem.initialize(self, uri)
  self.parsedUri = url.parse(uri)
end

function WebHdfsFileSystem:listStatus(path)
  local urlSegments = moses.clone(self.parsedUri)
  urlSegments.path = urlSegments.path .. path
  urlSegments.query = 'op=LISTSTATUS'
  local uri = url.build(urlSegments)
  local json = http.request(uri)
  local obj = jsonutil.decode(json)
  return obj.FileStatuses.FileStatus
end

function WebHdfsFileSystem:open(path)
  local urlSegments = moses.clone(self.parsedUri)
  urlSegments.path = urlSegments.path .. path
  urlSegments.query = 'op=OPEN'
  local uri = url.build(urlSegments)
  local data = http.request(uri)
  return data
end

return WebHdfsFileSystem
