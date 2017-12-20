local Context = require 'stuart.Context'
local isInstanceOf = require 'stuart.util.isInstanceOf'
local moses = require 'stuart.util.moses'
local SparkConf = require 'stuart.SparkConf'
local StreamingContext = require 'stuart.streaming.StreamingContext'

local Stuart = {}

Stuart.NewContext = function(master, appName)
  return Context:new(master, appName)
end

Stuart.NewStreamingContext = function(arg1, arg2, arg3, arg4)
  if moses.isString(arg1) and (moses.isString(arg2) or arg2 == nil) and moses.isNumber(arg3) then
    local sc = Context:new(arg1, arg2, arg4)
    return StreamingContext:new(sc, arg3)
  end
  if (moses.isString(arg1) or isInstanceOf(arg1, SparkConf)) and moses.isNumber(arg2) and arg3 == nil then
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
