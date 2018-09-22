local Logger = require 'stuart.internal.Logger'

describe('internal.Logger', function()

  local logger = Logger:new()
  
  it('can log', function()
    logger:error('Harmless test of error logging')
  end)
  
  it('can log() when no io module is present (regression test issue-73)', function()
    _G.io = nil
    logger:error('Harmless test of error logging')
  end)
  
end)
