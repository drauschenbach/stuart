local Logger = require 'stuart.internal.Logger'

local M = {log=Logger:new()}

function M.logInfo(msg)
  M.log:info(msg)
end

function M.logDebug(msg)
  M.log:debug(msg)
end

function M.logTrace(msg)
  M.log:trace(msg)
end

function M.logWarning(msg)
  M.log:warn(msg)
end

function M.logError(msg)
  M.log:error(msg)
end

return M
