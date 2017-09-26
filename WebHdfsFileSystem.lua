local class = require 'middleclass'
local FileSystem = require 'FileSystem'
local http = require 'socket.http'
local lunajson = require 'lunajson'
local moses = require 'moses'
local url = require 'socket.url'

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
  local obj = lunajson.decode(json)
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
