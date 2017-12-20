local has_luasocket, socket = pcall(require, 'socket')

local M = {}

if has_luasocket then
  M.precision = 4
  M.now = socket.gettime
elseif os ~= nil then
  M.precision = 0
  M.now = function() return os.time(os.date('*t')) end
else
  error('No clock capability')
end

return M
