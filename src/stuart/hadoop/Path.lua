local class = require 'middleclass'
local isInstanceOf = require 'stuart.util.isInstanceOf'
local netUrl = require 'net.url'

local function lastIndexOf(haystack, needle)
  local last_index = 0
  while haystack:sub(last_index+1, haystack:len()):find(needle) ~= nil do
    last_index = last_index + haystack:sub(last_index+1, haystack:len()):find(needle)
  end
  return last_index
end

local Path = class('Path')

function Path:initialize(arg1, arg2)
  if arg2 == nil then -- arg1=pathString
    self:_checkPathArg(arg1)
    self.uri = netUrl.parse(arg1)
    self:normalize()
  else -- arg1=parent, arg2=child
    local parent, child = arg1, arg2
    if not isInstanceOf(parent, Path) then parent = Path:new(parent) end
    if not isInstanceOf(child, Path) then child = Path:new(child) end
    -- resolve a child path against a parent path
    parent.uri.path = parent.uri.path .. '/'
    self.uri = netUrl.resolve(parent.uri, child.uri)
    self:normalize(parent:isAbsolute())
  end
end

function Path:_checkPathArg(path)
  -- disallow construction of a Path from an empty string
  assert(path ~= nil, 'Can not create a Path from a nil string')
  assert(#path > 0, 'Can not create a Path from an empty string')
end

 function Path:getName()
  local slash = lastIndexOf(self.uri.path, '/')
  if slash < 1 then return self.uri.path end
  return self.uri.path:sub(slash+1)
 end
 
 function Path:getParent()
  local path = self.uri.path
  local lastSlash = lastIndexOf(path, '/')
  local start = 1
  if #path == start or (lastSlash == start and #path == start+1) then -- at root
    return nil
  end
  local parent
  if lastSlash == nil then
    parent = '.'
  else
    local end_
    if lastSlash == start then end_ = start else end_ = lastSlash-1 end
    parent = path:sub(1, end_)
  end
  local p = Path:new(parent)
  p.uri.authority = self.uri.authority
  p.uri.scheme = self.uri.scheme
  return p
end

function Path:isAbsolute()
  return self.uri.path:sub(1,1) == '/'
end

function Path:normalize(isAbsolute)
  -- normalize using url module, while preventing it from turning a relative path into absolute
  if isAbsolute == nil then isAbsolute = self:isAbsolute() end
  self.uri = self.uri:normalize()
  if isAbsolute ~= self:isAbsolute() then
    self.uri.path = self.uri.path:sub(2)
  end
  -- trim trailing slash
  if #self.uri.path > 1 and self.uri.path:sub(#self.uri.path, #self.uri.path) == '/' then
    self.uri.path = self.uri.path:sub(1, #self.uri.path-1)
  end
end

function Path:toString()
  local buffer = {}
  if self.uri.scheme and #self.uri.scheme > 0 then
    buffer[#buffer+1] = self.uri.scheme .. ':'
  end
  if self.uri.authority and #self.uri.authority > 0 then
    buffer[#buffer+1] = '//' .. self.uri.authority
  end
  if self.uri.path and #self.uri.path > 0 then
    buffer[#buffer+1] = self.uri.path
  end
  if self.uri.fragment then
    buffer[#buffer+1] = '#'  .. self.uri.fragment
  end
  return table.concat(buffer, '')
end

function Path:toUri()
  return self.uri
end

return Path
