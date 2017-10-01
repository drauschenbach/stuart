local class = require 'middleclass'

local Receiver = class('Receiver')

function Receiver:initialize(ssc)
  self.ssc = ssc
end

function Receiver:onStart()
end

function Receiver:onStop()
end

function Receiver:run()
end

return Receiver
