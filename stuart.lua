Stuart = {}
Stuart.Context = require 'Context'
Stuart.StreamingContext = require 'StreamingContext'

Stuart.NewContext = function()
  return Stuart.Context:new()
end

Stuart.NewStreamingContext = function(sc, batchDuration)
  return Stuart.StreamingContext:new({sc=sc, batchDuration=batchDuration})
end

return Stuart
