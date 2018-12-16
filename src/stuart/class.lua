-- adapted from https://github.com/stevedonovan/Microlight#classes (MIT License)
-- external API adapted roughly to https://github.com/torch/class

local M = {}

function M.istype(obj, super)
  return super.classof(obj)
end

function M.new(base)
  local klass, base_ctor = {}, nil
  if base then
    for k,v in pairs(base) do klass[k]=v end
    klass._base = base
    base_ctor = rawget(base,'_init') or function() end
  end
  klass.__index = klass
  klass._class = klass
  klass.classof = function(obj)
    local m = getmetatable(obj) -- an object created by class() ?
    if not m or not m._class then return false end
    while m do -- follow the inheritance chain
      if m == klass then return true end
      m = rawget(m,'_base')
    end
    return false
  end
  klass.new = function(...)
    local obj = setmetatable({},klass)
    if rawget(klass,'_init') then
      local res = klass._init(obj,...) -- call our constructor
      if res then -- which can return a new self
        obj = setmetatable(res,klass)
      end
    elseif base_ctor then -- call base ctor automatically
        base_ctor(obj,...)
    end
    return obj
  end
  --setmetatable(klass, {__call=klass.new})
  return klass
end

return M
