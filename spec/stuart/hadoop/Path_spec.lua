local Path = require 'stuart.hadoop.Path'
local URL = require 'net.url'

local function toStringTest(pathString)
  assert.equal(pathString, Path:new(pathString):toString())
end

describe('Hadoop Path', function()

  it('toString()', function()
    toStringTest('/')
    toStringTest('/foo')
    toStringTest('/foo/bar')
    toStringTest('foo')
    toStringTest('foo/bar')
    toStringTest('/foo/bar#boo')
    toStringTest('foo/bar#boo')
    assert.has_error(function() Path:new('') end)
  end)
  
  it('isAbsolute()', function()
    assert.is_true(Path:new('/'):isAbsolute())
    assert.is_true(Path:new('/foo'):isAbsolute())
    assert.is_not_true(Path:new('foo'):isAbsolute())
    assert.is_not_true(Path:new('foo/bar'):isAbsolute())
    assert.is_not_true(Path:new('.'):isAbsolute())
  end)
  
  it('parent()', function()
    assert.same(Path:new('/foo'), Path:new('/foo/bar'):getParent())
    assert.same(Path:new('foo'), Path:new('foo/bar'):getParent())
    assert.same(Path:new('/'), Path:new('/foo'):getParent())
    assert.is_nil(Path:new('/'):getParent())
  end)
  
  it('dots', function()
    -- Test Path(String)
    assert.equal('/foo/bar/baz', Path:new('/foo/bar/baz'):toString())
--    assert.equal('/foo/bar', Path:new('/foo/bar', '.'):toString())
    assert.equal('/foo/baz', Path:new('/foo/bar/../baz'):toString())
    assert.equal('/foo/bar/baz', Path:new('/foo/bar/./baz'):toString())
    assert.equal('/foo/fud', Path:new('/foo/bar/baz/../../fud'):toString())
    assert.equal('/foo/fud', Path:new('/foo/bar/baz/.././../fud'):toString())
--    assert.equal('../../foo/bar', Path:new('../../foo/bar'):toString())
--    assert.equal('../../foo/bar', Path:new('.././../foo/bar'):toString())
    assert.equal('foo/bar/baz', Path:new('./foo/bar/baz'):toString())
    assert.equal('/baz/boo', Path:new('/foo/bar/../../baz/boo'):toString())
    --assert.equal('foo/bar', Path:new('foo/bar/'):toString())
    assert.equal('foo/baz', Path:new('foo/bar/../baz'):toString())
    assert.equal('baz/boo', Path:new('foo/bar/../../baz/boo'):toString())
    
    -- Test Path(Path,Path)
--    assert.equal('/foo/bar/baz/boo', Path:new('/foo/bar', 'baz/boo'):toString())
    assert.equal('foo/bar/baz/bud', Path:new('foo/bar/','baz/bud'):toString())
    
    --assert.equal('/boo/bud', Path:new('/foo/bar','../../boo/bud'):toString())
    --assert.equal('boo/bud', Path:new('foo/bar','../../boo/bud'):toString())
    --assert.equal('boo/bud', Path:new('.','boo/bud'):toString())
    
    --assert.equal('/foo/boo/bud', Path:new('/foo/bar/baz','../../boo/bud'):toString())
    --assert.equal('foo/boo/bud', Path:new('foo/bar/baz','../../boo/bud'):toString())
    
    --assert.equal('../../../../boo/bud', Path:new('../../','../../boo/bud'):toString())
    --assert.equal('../../../../boo/bud', Path:new('../../foo','../../../boo/bud'):toString())
    --assert.equal('../../foo/boo/bud', Path:new('../../foo/bar','../boo/bud'):toString())
    
    --assert.equal('', Path:new('foo/bar/baz','../../..'):toString())
    --assert.equal('../..', Path:new('foo/bar/baz','../../../../..'):toString())
  end)
  
  -- Test URIs created from Path objects
  it('path to URI conversion', function()
    -- Path differs from URI in that it ignores the query part..
--    assertEquals('? mark char in to URI',
--            URL.parse(null, null, '/foo?bar', null, null),
--            Path:new('/foo?bar'):toUri())
--    assertEquals('escape slashes chars in to URI',
--            URL.parse(null, null, '/foo\'bar', null, null),
--            Path:new('/foo\'bar'):toUri())
--    assertEquals('spaces in chars to URI',
--            URL.parse(null, null, '/foo bar', null, null),
--            Path:new('/foo bar'):toUri())
    
    -- therefore 'foo?bar' is a valid Path, so a URI created from a Path
    -- has path 'foo?bar' where in a straight URI the path part is just 'foo'
--    assert.equal('/foo?bar', Path:new('http://localhost/foo?bar'):toUri().path)
    assert.equal('/foo', URL.parse('http://localhost/foo?bar').path)

    -- The path part handling in Path is equivalent to URI
    assert.equal(URL.parse('/foobar').path, Path:new('/foobar'):toUri().path)
    assert.same(URL.parse('/foobar'), Path:new('/foobar'):toUri())
    assert.same(URL.parse('/foo+bar'), Path:new('/foo+bar'):toUri())
    assert.same(URL.parse('/foo-bar'), Path:new('/foo-bar'):toUri())
    assert.same(URL.parse('/foo=bar'), Path:new('/foo=bar'):toUri())
    assert.same(URL.parse('/foo,bar'), Path:new('/foo,bar'):toUri())
  end)
  
  it('getName()', function()
    assert.equal('', Path:new('/'):getName())
    assert.equal('foo', Path:new('foo'):getName())
    assert.equal('foo', Path:new('/foo'):getName())
    assert.equal('foo', Path:new('/foo/'):getName())
    assert.equal('bar', Path:new('/foo/bar'):getName())
    assert.equal('bar', Path:new('hdfs:///host/foo/bar'):getName())
  end)
  
end)
