local WebHdfsFileSystem = require 'stuart.WebHdfsFileSystem'
local moses = require 'moses'

describe('LocalFileSystem', function()

  it('isDirectory() positive match', function()
    local webhdfsUrl = os.getenv('WEBHDFS_URL')
    if not webhdfsUrl then return pending('No WEBHDFS_URL is configured') end
    local fs = WebHdfsFileSystem:new(webhdfsUrl)
    assert.equal(true, fs:mkdirs('/foo'))
    assert.equal(true, fs:isDirectory('/foo'))
  end)

  it('listStatus() a directory', function()
    local webhdfsUrl = os.getenv('WEBHDFS_URL')
    if not webhdfsUrl then return pending('No WEBHDFS_URL is configured') end
    local fs = WebHdfsFileSystem:new(webhdfsUrl)
    assert.equal(true, fs:mkdirs('/foo/bar'))
    local fileStatuses = fs:listStatus('/foo')
    
    local fileStatus = moses.findWhere(fileStatuses, {pathSuffix='bar'})
    assert.is_not_nil(fileStatus)
    assert.equal('DIRECTORY', fileStatus.type)
  end)

end)
