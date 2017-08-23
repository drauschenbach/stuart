local moses = require 'moses'

Stuart = {}
Stuart.Context = require 'Context'
Stuart.StreamingContext = require 'StreamingContext'

Stuart.NewContext = function()
  return Stuart.Context:new()
end

Stuart.NewStreamingContext = function(arg1, arg2, arg3)
  if moses.isString(arg1) and (moses.isString(arg2) or arg2 == nil) and moses.isNumber(arg3) then
    local sc = Stuart.Context:new{master=arg1, appName=arg2}
    return Stuart.StreamingContext:new(sc, arg3)
  end
  if moses.isString(arg1) and moses.isNumber(arg2) and arg3 == nil then
    local sc = Stuart.Context:new{master=arg1, appName=nil}
    return Stuart.StreamingContext:new(sc, arg2)
  end
  if moses.isTable(arg1) and moses.isNumber(arg2) and arg3 == nil then
    return Stuart.StreamingContext:new(arg1, arg2)
  end
  if moses.isTable(arg1) then
    return Stuart.StreamingContext:new(arg1)
  end
  error('Failed detecting NewStreamingContext parameters')
end

return Stuart
