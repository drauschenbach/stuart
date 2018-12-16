-- don't pollute class_spec.lua encapsulation with dependencies on streaming modules
describe('class regression test #119', function()
  
  it('Three levels of subclassing works', function()
    local ssc = {sc={}}
    
    local DStream = require 'stuart.streaming.DStream'
    local dstream = DStream.new(ssc)
    assert.is_not_nil(dstream.inputs)
    
    local SocketInputDStream = require 'stuart.streaming.SocketInputDStream'
    local hostname = ''
    local port = 5000
    dstream = SocketInputDStream.new(ssc, hostname, port)

    local QueueInputDStream = require 'stuart.streaming.QueueInputDStream'
    dstream = QueueInputDStream.new(ssc, {}, true)
    assert.is_not_nil(dstream.inputs)

  end)
  
end)
