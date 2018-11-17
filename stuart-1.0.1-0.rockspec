package = "stuart"
version = "1.0.1-0"
source = {
   url = "https://github.com/BixData/stuart/archive/1.0.1-0.tar.gz",
   dir = "stuart-1.0.1-0"
}
description = {
   summary = "An Apache Spark runtime for embedding and edge computing",
   detailed = [[
      A pure Lua rewrite of Apache Spark, designed for
      embedding and edge computing.
   ]],
   homepage = "https://github.com/BixData/stuart",
   maintainer = "David Rauschenbach",
   license = "Apache 2.0"
}
dependencies = {
   "lua >= 5.1",
   "lunajson ~> 1.2",
   "moses ~> 2.1.0",
   "net-url ~> 0.9"
}
build = {
   type = "builtin",
   modules = {
      stuart = "src/stuart.lua",
      ["stuart.class"] = "src/stuart/class.lua",
      ["stuart.Context"] = "src/stuart/Context.lua",
      ["stuart.FileSystem"] = "src/stuart/FileSystem.lua",
      ["stuart.fileSystemFactory"] = "src/stuart/fileSystemFactory.lua",
      ["stuart.LocalFileSystem"] = "src/stuart/LocalFileSystem.lua",
      ["stuart.Partition"] = "src/stuart/Partition.lua",
      ["stuart.RDD"] = "src/stuart/RDD.lua",
      ["stuart.SparkConf"] = "src/stuart/SparkConf.lua",
      ["stuart.WebHdfsFileSystem"] = "src/stuart/WebHdfsFileSystem.lua",
      
      ["stuart.hadoop.Path"] = "src/stuart/hadoop/Path.lua",
      
      ["stuart.interface"] = "src/stuart/interface.lua",
      
      ["stuart.internal.Logger"] = "src/stuart/internal/Logger.lua",
      ["stuart.internal.logging"] = "src/stuart/internal/logging.lua",
      
      ["stuart.streaming.DStream"] = "src/stuart/streaming/DStream.lua",
      ["stuart.streaming.HttpReceiver"] = "src/stuart/streaming/HttpReceiver.lua",
      ["stuart.streaming.QueueInputDStream"] = "src/stuart/streaming/QueueInputDStream.lua",
      ["stuart.streaming.Receiver"] = "src/stuart/streaming/Receiver.lua",
      ["stuart.streaming.ReceiverInputDStream"] = "src/stuart/streaming/ReceiverInputDStream.lua",
      ["stuart.streaming.SocketInputDStream"] = "src/stuart/streaming/SocketInputDStream.lua",
      ["stuart.streaming.SocketReceiver"] = "src/stuart/streaming/SocketReceiver.lua",
      ["stuart.streaming.StreamingContext"] = "src/stuart/streaming/StreamingContext.lua",
      ["stuart.streaming.TransformedDStream"] = "src/stuart/streaming/TransformedDStream.lua",
      ["stuart.streaming.WindowedDStream"] = "src/stuart/streaming/WindowedDStream.lua",
      
      ["stuart.util"] = "src/stuart/util.lua",
      ["stuart.util.spark"] = "src/stuart/util/spark.lua",
      ["stuart.util.spark.samplingUtils"] = "src/stuart/util/spark/samplingUtils.lua"
   }
}
