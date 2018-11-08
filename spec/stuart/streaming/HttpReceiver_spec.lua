local HttpReceiver = require 'stuart.streaming.HttpReceiver'
local registerAsserts = require 'registerAsserts'
local stuart = require 'stuart'

registerAsserts(assert)

describe('HttpReceiver', function()

  local sc = stuart.NewContext()
  local ssc = stuart.NewStreamingContext(sc)
  
  it('parses a status line', function()
    local receiver = HttpReceiver:new(ssc)
    local status, statusLine = receiver:parseStatusLine('HTTP/1.0 201 abc')
    assert.equals('201', status)
    assert.equals('201 abc', statusLine)
  end)
  
  it('parses header lines', function()
    local receiver = HttpReceiver:new(ssc)
    assert.same({}, receiver.responseHeaders)
    
    receiver:parseHeaderLine('Content-Type: text/plain')
    assert.same({['Content-Type']='text/plain'}, receiver.responseHeaders)
    
    receiver:parseHeaderLine('Length: 123')
    assert.equals('123', receiver.responseHeaders['Length'])
    assert.equals('text/plain', receiver.responseHeaders['Content-Type'])
  end)

end)
