-- Begin Redis support
local package = {preload={}}
local function require(name) return package.preload[name]() end
local arg = ARGV
local os = {}
-- End Redis support

