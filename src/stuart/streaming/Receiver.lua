local class = require 'stuart.class'

local Receiver = class.new()

function Receiver:_init(ssc)
  self.ssc = ssc
end

function Receiver:onStart()
end

function Receiver:onStop()
end

function Receiver:poll()
end

return Receiver
