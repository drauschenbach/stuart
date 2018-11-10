local M = {}

function M.class(super)
  local class = require 'stuart.class'
  return class.new(super)
end

function M.istype(obj, super)
  local class = require 'stuart.class'
  return class.istype(obj, super)
end

function M.NewContext(master, appName)
  local Context = require 'stuart.Context'
  return Context.new(master, appName)
end

function M.NewStreamingContext(arg1, arg2, arg3, arg4)
  local Context = require 'stuart.Context'
  local moses = require 'moses'
  local StreamingContext = require 'stuart.streaming.StreamingContext'
  if moses.isString(arg1) and (moses.isString(arg2) or arg2 == nil) and moses.isNumber(arg3) then
    local sc = Context.new(arg1, arg2, arg4)
    return StreamingContext.new(sc, arg3)
  end
  local SparkConf = require 'stuart.SparkConf'
  local istype = require 'stuart.class'.istype
  if (moses.isString(arg1) or istype(arg1, SparkConf)) and moses.isNumber(arg2) and arg3 == nil then
    local sc = Context.new(arg1)
    return StreamingContext.new(sc, arg2)
  end
  
  if moses.isTable(arg1) then
    if moses.isNumber(arg2) then
      return StreamingContext.new(arg1, arg2)
    end
    return StreamingContext.new(arg1)
  end
  
  error('Failed detecting NewStreamingContext parameters')
end

return M
