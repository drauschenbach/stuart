FROM ubuntu:16.04

# Install package dependencies
#   - Lua plus C headers for LuaRocks modules
#   - LuaRocks
RUN apt-get update && apt-get install -y \
    lua5.2 \
    liblua5.2-dev \
    luarocks

# Install LuaRocks modules required for testing
RUN luarocks install busted
RUN luarocks install luasocket
RUN luarocks install lunajson
RUN luarocks install middleclass
RUN luarocks install moses
RUN luarocks install net-url

# Add this project
ADD . /app
WORKDIR /app
