local class = require 'middleclass'
local moses = require 'moses'

local SparkConf = class('SparkConf')

function SparkConf:initialize()
  self.settings = {}
  getmetatable(self).getOption = self.get
end

function SparkConf:appName()
  return self:get('spark.app.name')
end

function SparkConf:clone()
  local cloned = SparkConf:new()
  cloned.settings = moses.clone(self.settings)
  return cloned
end

function SparkConf:contains(key)
  return self.settings[key] ~= nil
end

function SparkConf:get(key, defaultValue)
  return self.settings[key] or defaultValue
end

function SparkConf:getAppId()
  return self:get('spark.app.id')
end

function SparkConf:getAll()
  local t = {}
  for k,v in pairs(self.settings) do
    t[#t+1] = {k, v}
  end
  return t
end

function SparkConf:getBoolean(key, defaultValue)
  local s = self.settings[key]
  if s == 'true' then return true end
  if s == 'false' then return false end
  return defaultValue
end

function SparkConf:master()
  return self:get('spark.master')
end

function SparkConf:remove(key)
  self.settings[key] = nil
  return self
end

function SparkConf:set(key, value)
  assert(key ~= nil)
  assert(value ~= nil)
  
  self.settings[key] = value
  return self
end

function SparkConf:setAll(settings)
  for _,setting in ipairs(settings) do
    local k,v = setting[1], setting[2]
    self:set(k,v)
  end
  return self
end

function SparkConf:setAppName(name)
  self:set('spark.app.name', name)
  return self
end

function SparkConf:setIfMissing(key, value)
  if self.settings[key] == nil then
    self.settings[key] = value
  end
  return self
end

function SparkConf:setMaster(master)
  self:set('spark.master', master)
  return self
end

function SparkConf:setSparkHome(home)
  self:set('spark.home', home)
  return self
end

function SparkConf:toDebugString()
  local s = ''
  for k,v in pairs(self.settings) do
    s = s .. k .. '=' .. v .. '\n'
  end
  return s
end

return SparkConf
