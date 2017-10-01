---
-- lodash for lua
-- @author Daniel Moghimi (daniel.mogimi@gmail.com)
-- @license MIT
--
-- adapted to moses for the Stuart project
---

local moses = require 'moses'

---
-- Checks if n is between start and up to but not including, end.
-- If end is not specified it’s set to start with start then set to 0.
-- @usage print(_.inRange(-3, -4, 8))
-- --> true
--
-- @param n The number to check.
-- @param start The start of the range.
-- @param stop The end of the range.
-- @return Returns true if n is in the range, else false.
local inRange = function (n, start, stop)
    local _start = moses.isNil(stop) and 0 or start or 0
    local _stop = moses.isNil(stop) and start or stop or 1
    return n >= _start and n < _stop
end

return inRange
