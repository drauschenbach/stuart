local registerAsserts = require 'registerAsserts'
local jsonDecode = require 'stuart.util'.jsonDecode

registerAsserts(assert)

describe('util.json', function()

  it('can decode a scalar', function()
    local actual = jsonDecode('7')
    assert.equal(7, actual)
  end)

  it('can decode a scalar using cjson', function()
    local has_cjson, _ = pcall(require, 'cjson')
    if not has_cjson then return pending('cjson not installed') end
    local actual = jsonDecode('7')
    assert.equal(7, actual)
  end)

  it('can decode an object', function()
    local actual = jsonDecode('{"a":1,"b":"2"}')
    assert.same({a=1,b='2'}, actual)
  end)

  it('can decode an object using cjson', function()
    local has_cjson, _ = pcall(require, 'cjson')
    if not has_cjson then return pending('cjson not installed') end
    local actual = jsonDecode('{"a":1,"b":"2"}')
    assert.same({a=1,b='2'}, actual)
  end)

end)
