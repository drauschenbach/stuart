local has_luasocket, socket = pcall(require, 'socket')

if has_luasocket then
  return socket.sleep
end

-- This environment is not capable of sleeping. Spark Streaming control loops
-- will peg the CPU.
return function() end
