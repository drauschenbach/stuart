local has_cjson, cjson = pcall(require, 'cjson')
local lunajsonDecoder = require 'lunajson.decoder'

local M = {}

M.decode = function(text)
  if has_cjson then
    return cjson.decode(text)
  else
    return lunajsonDecoder(text)
  end
end

return M
