local floor = math.floor
local max = math.max
local t_insert = table.insert

--- Produces a flexible list of numbers. If one positive value is passed, will count from 0 to that value,
-- with a default step of 1. If two values are passed, will count from the first one to the second one, with the
-- same default step of 1. A third value passed will be considered a step value.
-- @name range
-- @param[opt] from the initial value of the range
-- @param[optchain] to the final value of the range
-- @param[optchain] step the step of count
-- @return a new array of numbers
local range = function(...)
  local arg = {...}
  local _start,_stop,_step
  if #arg==0 then return {}
  elseif #arg==1 then _stop,_start,_step = arg[1],0,1
  elseif #arg==2 then _start,_stop,_step = arg[1],arg[2],1
  elseif #arg == 3 then _start,_stop,_step = arg[1],arg[2],arg[3]
  end
  if (_step and _step==0) then return {} end
  
  -- BEGIN patch --------------------------------------------------------------
  if _start == 1 and _stop == 1 and _step == 1 then return {1} end
  -- END patch ----------------------------------------------------------------
  
  local _ranged = {}
  local _steps = max(floor((_stop-_start)/_step),0)
  for i=1,_steps do _ranged[#_ranged+1] = _start+_step*i end
  if #_ranged>0 then t_insert(_ranged,1,_start) end
  return _ranged
end
  
return range
