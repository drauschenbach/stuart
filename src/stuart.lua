local moses = require 'moses'

local Context = require 'stuart.Context'
local StreamingContext = require 'stuart.streaming.StreamingContext'

Stuart = {}

Stuart.NewContext = function()
  return Context:new()
end

Stuart.NewStreamingContext = function(arg1, arg2, arg3)
  if moses.isString(arg1) and (moses.isString(arg2) or arg2 == nil) and moses.isNumber(arg3) then
    local sc = Context:new(arg1, arg2)
    return StreamingContext:new(sc, arg3)
  end
  if moses.isString(arg1) and moses.isNumber(arg2) and arg3 == nil then
    local sc = Context:new(arg1)
    return StreamingContext:new(sc, arg2)
  end
  
  if moses.isTable(arg1) then
    if moses.isNumber(arg2) then
      return StreamingContext:new(arg1, arg2)
    end
    return StreamingContext:new(arg1)
  end
  
  error('Failed detecting NewStreamingContext parameters')
end

return Stuart
