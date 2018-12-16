local class = require 'stuart.class'
local Receiver = require 'stuart.streaming.Receiver'

-- Receiver capable of tailing an http chunked stream
local HttpReceiver = class.new(Receiver)

function HttpReceiver:_init(ssc, url, mode, requestHeaders)
  Receiver._init(self, ssc)
  self.url = url
  self.mode = mode or 'text' -- 'text' or 'binary'
  self.requestHeaders = requestHeaders or {}
  self.responseHeaders = {}
  self.state = 0 -- 0=receive status line, 1=receive headers, 2=receive content
end

function HttpReceiver:onHeadersReceived()
end

function HttpReceiver:onStart()
  local has_luasocket, socket = pcall(require, 'socket')
  assert(has_luasocket)
  local _, socketUrl = pcall(require, 'socket.url')
  local parsedUrl = socketUrl.parse(self.url)
  local log = require 'stuart.internal.logging'.log
  log:info(string.format('Connecting to %s:%d', parsedUrl.host, parsedUrl.port))
  self.conn, self.err = socket.connect(parsedUrl.host, parsedUrl.port)
  if self.conn ~= nil then
    log:info(string.format('Connected to %s:%d', parsedUrl.host, parsedUrl.port))
    -- send GET request
    local url = parsedUrl.path
    if parsedUrl.query ~= nil then url = url .. '?' .. parsedUrl.query end
    if parsedUrl.fragment ~= nil then url = url .. '#' .. parsedUrl.fragment end
    local header = table.concat(self.requestHeaders, '\r\n')
    --print('GET ' .. url)
    self.conn:send('GET ' .. url .. ' HTTP/1.0\r\n' .. header .. '\r\n\r\n')
  else
    log:error(string.format('Error connecting to %s:%d: %s', parsedUrl.host, parsedUrl.port, self.err))
  end
end

function HttpReceiver:onStop()
  if self.conn ~= nil then self.conn:close() end
end

function HttpReceiver:parseStatusLine(line)
  local i = line:find(' ')
  local statusLine = line:sub(i+1)
  local j = statusLine:find(' ')
  local status = statusLine:sub(1, j-1)
  return status, statusLine
end

function HttpReceiver:parseHeaderLine(line)
  local i = line:find(': ')
  if i ~= nil then
    local name = line:sub(1, i-1)
    local value = line:sub(i+2)
    self.responseHeaders[name] = value
  end
end

function HttpReceiver:poll(durationBudget)
  local now = require 'stuart.interface'.now
  local startTime = now()
  local data = {}
  local minWait = 0.01
  while true do
    local elapsed = now() - startTime
    if elapsed > durationBudget then break end
    
    self.conn:settimeout(math.max(minWait, durationBudget - elapsed))
    if self.mode == 'text' then
      local line, err = self.conn:receive('*l')
      if not err then
        if self.state == 0 then
          self.status, self.statusLine = self:parseStatusLine(line)
          self.state = 1
        elseif self.state == 1 then
          if line ~= '' then
            self:parseHeaderLine(line)
          else
            pcall(function() self:onHeadersReceived(self.responseHeaders) end)
            self.state = 2 -- blank line indicates last header received
          end
        else
          line = self:transform(line)
          data[#data+1] = line
        end
      end
    else
      error('binary mode not implemented yet')
    end
  end
  if #data == 0 then return nil end
  return {self.ssc.sc:makeRDD(data)}
end

function HttpReceiver:transform(data)
  return data
end

return HttpReceiver
