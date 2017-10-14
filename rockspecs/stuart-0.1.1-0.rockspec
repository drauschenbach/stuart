package = "stuart"
version = "0.1.1-0"
source = {
   url = "https://github.com/BixData/stuart/archive/0.1.1-0.tar.gz",
   dir = "stuart-0.1.1-0"
}
description = {
   summary = "An Apache Spark runtime for embedding and edge computing",
   detailed = [[
      A native Lua implementation of Apache Spark, designed for
      embedding and edge computing.
   ]],
   homepage = "https://github.com/BixData/stuart",
   maintainer = "David Rauschenbach",
   license = "Apache 2.0"
}
dependencies = {
   "lua >= 5.1",
   "luasocket <= 3.0rc1-2",
   "lunajson <= 1.2-0",
   "middleclass <= 4.1-0",
   "moses <= 1.6.1-1"
}
build = {
   type = "builtin",
   modules = {
      stuart = "src/stuart.lua",
      ["stuart.Context"] = "src/stuart/Context.lua",
      ["stuart.FileSystem"] = "src/stuart/FileSystem.lua",
      ["stuart.LocalFileSystem"] = "src/stuart/LocalFileSystem.lua",
      ["stuart.Partition"] = "src/stuart/Partition.lua",
      ["stuart.RDD"] = "src/stuart/RDD.lua",
      ["stuart.WebHdfsFileSystem"] = "src/stuart/WebHdfsFileSystem.lua",
      ["stuart.fileSystemFactory"] = "src/stuart/fileSystemFactory.lua",
      ["stuart.streaming.DStream"] = "src/stuart/streaming/DStream.lua",
      ["stuart.streaming.HttpReceiver"] = "src/stuart/streaming/HttpReceiver.lua",
      ["stuart.streaming.QueueInputDStream"] = "src/stuart/streaming/QueueInputDStream.lua",
      ["stuart.streaming.Receiver"] = "src/stuart/streaming/Receiver.lua",
      ["stuart.streaming.ReceiverInputDStream"] = "src/stuart/streaming/ReceiverInputDStream.lua",
      ["stuart.streaming.SocketInputDStream"] = "src/stuart/streaming/SocketInputDStream.lua",
      ["stuart.streaming.SocketReceiver"] = "src/stuart/streaming/SocketReceiver.lua",
      ["stuart.streaming.StreamingContext"] = "src/stuart/streaming/StreamingContext.lua",
      ["stuart.streaming.TransformedDStream"] = "src/stuart/streaming/TransformedDStream.lua",
      ["stuart.util.json"] = "src/stuart/util/json.lua"
   }
}
