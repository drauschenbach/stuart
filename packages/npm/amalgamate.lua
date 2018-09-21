local rockspecFile = arg[1]
if rockspecFile == nil then
  error('A rockspec argument is required')
end


-- Run the rockspec file, which sets certain global variables
local rockspecFn = assert(loadfile(rockspecFile))
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
    -- TODO
  end 
end


-- Write amalg.cache file
local file = io.open('amalg.cache', 'w')
file:write('return {\n')
for k,v in pairs(amalgCache) do
  file:write('  ["' .. k .. '"] = "L",\n')
end
file:write('}\n')
file:close()


-- Invoke amalg.lua to produce a single Lua file
os.execute('amalg.lua -s empty.lua -o stuart.lua -c')
