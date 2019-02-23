-- https://github.com/apache/spark/blob/v2.2.0/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala
local moses = require 'moses'
local SparkContext = require 'stuart.Context'

local sc = SparkContext.new('local[1]', 'Spark Pi')
local slices = 2
if arg ~= nil and #arg > 0 then slices = arg[1] end

local NUM_SAMPLES = 5000
local n = 100000 * slices
local count = sc:parallelize(moses.range(1,n), slices):map(function(_,i)
  local x = math.random() * 2 - 1
  local y = math.random() * 2 - 1
  if x*x + y*y <= 1 then return 1 else return 0 end
end):reduce(function(r,x) return r+x end)
local msg = string.format('Pi is roughly %.9f', 4*count/(n-1))
print(msg) 
return msg
