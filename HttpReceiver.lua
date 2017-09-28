local class = require 'middleclass'
local Receiver = require 'Receiver'
local socket = require 'socket'
local socketUrl = require 'socket.url'

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
  local rdds = {}
  local minWait = 0.02 -- never block less than 20ms
  while true do
    local elapsed = socket.gettime() - timeOfLastYield
    if elapsed > durationBudget then
      coroutine.yield(rdds)
      rdds = {}
      timeOfLastYield = socket.gettime()
    else
      self.conn:settimeout(math.max(minWait, durationBudget - elapsed))
      if self.mode == 'text' then
        local line, err = self.conn:receive('*l')
        if not err then
          if self.state == 0 then
            local i = line:find(' ')
            self.statusLine = line:sub(i+1)
            local j = self.statusLine:find(' ')
            self.status = self.statusLine:sub(1, j)
            self.state = 1
          elseif self.state == 1 then
            if line ~= '' then
              local i = line:find(': ')
              if i ~= nil then
                local name = line:sub(1, i-1)
                local value = line:sub(i+2)
                self.responseHeaders[name] = value
              end
            else
              self.state = 2 -- blank line indicates last header received
            end
          else
            line = self:transform(line)
            local rdd = self.ssc.sc:makeRDD({line})
            rdds[#rdds+1] = rdd
          end
        end
      else
        error('binary mode not implemented yet')
      end
    end
  end
end

function HttpReceiver:transform(data)
  return data
end

return HttpReceiver
