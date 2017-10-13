local has_cjson, cjson = pcall(require, 'cjson')
local lunajson = require 'lunajson'

local M = {}

M.decode = function(text)
  if has_cjson then
    return cjson.decode(text)
  else
    return lunajson.decode(text)
  end
end

M.encode = function(value)
  if has_cjson then
    return cjson.encode(value)
  else
    return lunajson.encode(value)
  end
end

return M
