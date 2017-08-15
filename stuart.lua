local moses = require 'moses'

Stuart = {}
Stuart.Context = require 'Context'
Stuart.StreamingContext = require 'StreamingContext'

Stuart.NewContext = function()
  return Stuart.Context:new()
end

Stuart.NewStreamingContext = function(arg1, arg2, arg3)
  if moses.isString(arg1) and moses.isString(arg2) and moses.isNumber(arg3) then
    local sc = Stuart.Context:new{master=mastr, appName=appName}
    return Stuart.StreamingContext:new({sc=sc, batchDuration=batchDuration})
  end
  if moses.isTable(arg1) then
    local sc = arg1
    return Stuart.StreamingContext:new({sc=sc})
  end
  error('Failed detecting NewStreamingContext parameters')
end

return Stuart
