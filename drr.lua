local _ = require 'lodash'
local class = require 'middleclass'
local HttpReceiver = require 'stuart.streaming.HttpReceiver'
local lunajson = require 'lunajson'
local moses = require 'moses'
local stuart = require 'stuart'

local namespaceFilter = 'CPU'
local instanceFilter = 'cpu2'
local fieldFilter = 'ProcessorUser'

-- Receiver -------------------------------------------------------------------

local MyReceiver = class('MyReceiver', HttpReceiver)

function MyReceiver:initialize(ssc, url)
  local headers = {'Accept: application/json'}
  HttpReceiver.initialize(self, ssc, url, 'text', headers)
end

function MyReceiver:transform(line)
  local record = lunajson.decode(line)
  local namespace = moses.findWhere(record.namespaces, {name=namespaceFilter})
  local instance = moses.findWhere(namespace.instances, {name=instanceFilter})
  local key = instance.keys[1]
  local processorUser = key.fields[fieldFilter]
  return processorUser
end

-- Spark Streaming Job --------------------------------------------------------

local sc = stuart.NewContext()
local ssc = stuart.NewStreamingContext(sc, .5)

local url = string.format('http://localhost:17071/v1/data/%s?instance=%s&field=%s&period=20ms', namespaceFilter, instanceFilter, fieldFilter)
local receiver = MyReceiver:new(ssc, url)
local dstream = ssc:receiverStream(receiver)
dstream:foreachRDD(function(rdd)
  _.print('Received RDD:', rdd:collect())
end)
ssc:start()
ssc:awaitTerminationOrTimeout(3)
ssc:stop()
