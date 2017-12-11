local fileSystemFactory = require 'stuart.fileSystemFactory'
local LocalFileSystem = require 'stuart.LocalFileSystem'
local WebHdfsFileSystem = require 'stuart.WebHdfsFileSystem'

describe('fileSystemFactory', function()

  describe('createForOpenPath()', function()
  
    it('returns a LocalFileSystem for relative path', function()
      local fs, openPath = fileSystemFactory.createForOpenPath('spec-fixtures/aristotle1.txt')
      assert.is_true(fs:isInstanceOf(LocalFileSystem))
      assert.equals('spec-fixtures/', fs:getUri())
      assert.equals('aristotle1.txt', openPath)
    end)
  
    it('returns a LocalFileSystem for relative directory path', function()
      local fs, openPath = fileSystemFactory.createForOpenPath('spec-fixtures/')
      assert.is_true(fs:isInstanceOf(LocalFileSystem))
      assert.equals('./', fs:getUri())
      assert.equals('spec-fixtures', openPath)
    end)
  
    it('returns a LocalFileSystem for a relative path starting with ./', function()
      local fs, openPath = fileSystemFactory.createForOpenPath('./spec-fixtures/aristotle1.txt')
      assert.is_true(fs:isInstanceOf(LocalFileSystem))
      assert.equals('./spec-fixtures/', fs:getUri())
      assert.equals('aristotle1.txt', openPath)
    end)
  
    it('returns a WebHdfsFileSystem for a webhdfs URL', function()
      local fs, openPath = fileSystemFactory.createForOpenPath('webhdfs://127.0.0.1:50070/webhdfs/v1/a/b/foo.txt?op=OPEN')
      assert.is_true(fs:isInstanceOf(WebHdfsFileSystem))
      assert.equals('webhdfs://127.0.0.1:50070/webhdfs/v1/', fs:getUri())
      assert.equals('a/b/foo.txt', openPath)
    end)

    it('injects /webhdfs/v1 for a WebHDFS URL that omits then', function()
      local fs, openPath = fileSystemFactory.createForOpenPath('webhdfs://127.0.0.1:50070/a/b/foo.txt?op=OPEN')
      assert.is_true(fs:isInstanceOf(WebHdfsFileSystem))
      assert.equals('webhdfs://127.0.0.1:50070/webhdfs/', fs:getUri())
      assert.equals('a/b/foo.txt', openPath)
    end)

  end)
  
end)
