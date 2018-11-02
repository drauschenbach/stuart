local M = {}

M.clockPrecision = function()
  local has_luasocket, _ = pcall(require, 'socket')
  if has_luasocket then
    return 4
  elseif os ~= nil then
    return 0 -- 0==whole seconds
  else
    error('No clock capability')
  end
end

M.now = function()
  local has_luasocket, socket = pcall(require, 'socket')
  if has_luasocket then
    return socket.gettime()
  elseif os ~= nil then
    return os.time(os.date('*t'))
  else
    error('No clock capability')
  end
end

M.sleep = function(duration)
  local has_luasocket, socket = pcall(require, 'socket')
  if has_luasocket then
    return socket.sleep(duration)
  end

  -- This environment is not capable of sleeping. Spark Streaming control loops
  -- will peg the CPU.
end

return M
