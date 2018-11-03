local class = require 'stuart.util.class'

local Receiver = class('Receiver')

function Receiver:__init(ssc)
  self.ssc = ssc
end

function Receiver:onStart()
end

function Receiver:onStop()
end

function Receiver:poll()
end

return Receiver
