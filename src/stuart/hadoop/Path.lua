local class = require 'middleclass'
local isInstanceOf = require 'stuart.util.isInstanceOf'
local url = require 'net.url'

local Path = class('Path')

function Path:initialize(arg1, arg2)
  if arg2 == nil then -- arg1=pathString
    self:_checkPathArg(arg1)
    self.uri = url.parse(arg1)
    self:normalize()
  else -- arg1=parent, arg2=child
    local parent, child = arg1, arg2
    if not isInstanceOf(parent, Path) then parent = Path:new(parent) end
    if not isInstanceOf(child, Path) then child = Path:new(child) end
    -- resolve a child path against a parent path
    self.uri = url.resolve(parent.uri, child.uri)
    self:normalize(parent:isAbsolute())
  end
end

function Path:_checkPathArg(path)
  -- disallow construction of a Path from an empty string
  assert(path ~= nil, 'Can not create a Path from a nil string')
  assert(#path > 0, 'Can not create a Path from an empty string')
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
