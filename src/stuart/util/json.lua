local has_cjson, cjson = pcall(require, 'cjson')
local lunajsonDecoder = require 'lunajson.decoder'

local M = {}

if has_cjson then
  M.decode = cjson.decode
else
  M.decode = lunajsonDecoder()
end

return M
