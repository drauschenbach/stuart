local LocalFileSystem = require 'stuart.LocalFileSystem'
local has_lfs = pcall(require, 'lfs')
local moses = require 'moses'

describe('LocalFileSystem', function()

  it('isDirectory() positive match', function()
    local fs = LocalFileSystem:new('spec-fixtures')
    assert.is_true(fs:isDirectory(''))
    
    fs = LocalFileSystem:new('./')
    assert.is_true(fs:isDirectory('spec-fixtures'))
  end)

  it('isDirectory() negative match', function()
    local fs = LocalFileSystem:new('spec-fixtures/')
    assert.is_false(fs:isDirectory('aristotle1.txt'))
  end)

  it('listStatus() a directory', function()
    if not has_lfs then return pending('lfs not installed') end
    
    local fs = LocalFileSystem:new('spec-fixtures')
    local fileStatuses = fs:listStatus()
    assert.is_true(#fileStatuses > 0)
    
    local fileStatus = moses.findWhere(fileStatuses, {path='aristotle1.txt'})
    assert.is_not_nil(fileStatus)
    assert.equal(false, fileStatus.isDirectory)
    assert.equal(true, fileStatus.isFile)
    assert.equal(false, fileStatus.isSymlink)
    
    fileStatus = moses.findWhere(fileStatuses, {path='.'})
    assert.is_not_nil(fileStatus)
    assert.equal(true, fileStatus.isDirectory)
    assert.equal(false, fileStatus.isFile)
    assert.equal(false, fileStatus.isSymlink)
  end)

end)
