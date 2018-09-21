-- Helper function
local function getModulesForDependency(dependency)
  local cmd = 'luarocks show ' .. dependency .. ' --modules'
  local f = assert(io.popen(cmd, 'r'))
  local s = assert(f:read('*a'))
  f:close()
  s = string.gsub(s, '^%s+', '')
  s = string.gsub(s, '%s+$', '')
  s = string.gsub(s, '[\n\r]+', ' ')
  local t = {}
  for k in string.gmatch(s, '%S+') do
    if k ~= 'moses_min' then
      t[#t+1] = k
    end
  end
  return t
end


-- main
local rockspecFilespec = arg[1]
if rockspecFilespec == nil then
  error('A rockspec argument is required')
end


-- Run the rockspec file, which sets certain global variables
local rockspecFn = assert(loadfile(rockspecFilespec))
rockspecFn()


-- Add modules for this rockspec to an amalg.cache file
local amalgCache = {}
for k,v in pairs(build.modules) do
  amalgCache[k] = 'L'
end


-- Add dependent modules for this rockspec to an amalg.cache file
for _,v in ipairs(dependencies) do
  local spaceAt = string.find(v, ' ')
  local dependency = v:sub(1, spaceAt-1)
  if dependency ~= 'lua' then
    local modules = getModulesForDependency(dependency)
    for _, module in ipairs(modules) do
      amalgCache[module] = 'L'
    end
  end 
end


-- Write amalg.cache file
local file = io.open('amalg.cache', 'w')
file:write('return {\n')
for k,v in pairs(amalgCache) do
  file:write('  ["' .. k .. '"] = "' .. v .. '",\n')
end
file:write('}\n')
file:close()


-- Invoke amalg.lua to produce a single stuart.lua file
os.execute('amalg.lua -s empty.lua -o stuart.lua -c')


-- Produce a stuart-min.lua file
-- TODO
