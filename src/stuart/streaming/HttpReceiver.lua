local class = require 'middleclass'
local socket = require 'socket'
local socketUrl = require 'socket.url'

local Receiver = require 'stuart.streaming.Receiver'

-- Receiver capable of tailing an http chunked stream
local HttpReceiver = class('HttpReceiver', Receiver)

function HttpReceiver:initialize(ssc, url, mode, requestHeaders)
  Receiver.initialize(self, ssc)
  self.url = url
  self.mode = mode or 'text' -- 'text' or 'binary'
  self.requestHeaders = requestHeaders or {}
  self.responseHeaders = {}
  self.state = 0 -- 0=receive status line, 1=receive headers, 2=receive content
end

function HttpReceiver:onHeadersReceived()
end

function HttpReceiver:onStart()
  local parsedUrl = socketUrl.parse(self.url)
  self.conn = socket.connect(parsedUrl.host, parsedUrl.port)
  if self.conn ~= nil then
    -- Connect and send GET request
    local url = parsedUrl.path
    if parsedUrl.query ~= nil then url = url .. '?' .. parsedUrl.query end
    if parsedUrl.fragment ~= nil then url = url .. '#' .. parsedUrl.fragment end
    local header = table.concat(self.requestHeaders, '\r\n')
    --print('GET ' .. url)
    self.conn:send('GET ' .. url .. ' HTTP/1.0\r\n' .. header .. '\r\n\r\n')
  end
end

function HttpReceiver:onStop()
  if self.conn ~= nil then self.conn:close() end
end

function HttpReceiver:run(durationBudget)
  local timeOfLastYield = socket.gettime()
  local data = {}
  local minWait = 0.02 -- never block less than 20ms
  while true do
    local elapsed = socket.gettime() - timeOfLastYield
    if elapsed > durationBudget then
      local rdd = self.ssc.sc:makeRDD(data)
      coroutine.yield({rdd})
      data = {}
      timeOfLastYield = socket.gettime()
    else
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
  end
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

function HttpReceiver:transform(data)
  return data
end

return HttpReceiver
