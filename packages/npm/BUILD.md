# Publishing to npmjs.com

## 1. Login

```sh
$ npm login
...
```

## 2. Prepare amalgamated Lua file

```sh
$ lua amalgamate.lua ../../rockspecs/stuart-0.1.7-1.rockspec
```

This generates `stuart.lua`, `package.json`, and `lua-stuart.tgz` files.

## 3. Upload to npmjs.com

```sh
$ npm publish lua-stuart.tgz
```
