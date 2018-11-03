FROM ubuntu:16.04

# Install package dependencies
#   - Lua plus C headers for LuaRocks modules
#   - LuaRocks
RUN apt-get update && apt-get install -y \
    lua5.3 \
    liblua5.3-dev \
    luarocks

# Install LuaRocks modules required for testing
RUN luarocks install busted
RUN luarocks install luasocket
RUN luarocks install lunajson 1.2
RUN luarocks install moses 2.1.0
RUN luarocks install net-url 0.9

# Add this project
ADD . /app
WORKDIR /app
