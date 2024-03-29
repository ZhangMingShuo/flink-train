package com.imooc.flink.course08


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object FileSystemSinkApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost",9999)
    data.print().setParallelism(1)
    val filePath = "src/test/data/hdfssink"
    val sink = new BucketingSink[String](filePath)
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter())
    sink.setBatchRolloverInterval(20)
    data.addSink(sink)
    env.execute("FileSystemSinkApp")
  }
}
