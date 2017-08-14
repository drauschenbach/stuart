local _ = require 'lodash'

Stuart = {}
Stuart.Context = require 'Context'
Stuart.StreamingContext = require 'StreamingContext'

Stuart.NewContext = function()
  return Stuart.Context:new()
end

Stuart.NewStreamingContext = function(arg1, arg2, arg3)
  if _.isString(arg1) and _.isString(arg2) and _.isNumber(arg3) then
    local sc = Stuart.Context:new{master=mastr, appName=appName}
    return Stuart.StreamingContext:new({sc=sc, batchDuration=batchDuration})
  end
  if _.isTable(arg1) then
    local sc = arg1
    return Stuart.StreamingContext:new({sc=sc})
  end
  error('Failed detecting NewStreamingContext parameters')
end

return Stuart
