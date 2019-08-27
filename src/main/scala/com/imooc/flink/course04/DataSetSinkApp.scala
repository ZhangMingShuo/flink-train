package com.imooc.flink.course04

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
object DataSetSinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1.to(10)
    val text = env.fromCollection(data)
    val filepath = "src/test/sink-out"
    text.writeAsText(filepath,WriteMode.OVERWRITE).setParallelism(2) //以覆盖的方式重写，如果不设置并行度为2的话，默认输出到文件；如果设置并行度为2，会在指定目录下面新建两个文件
    env.execute("DataSetSinkApp")
  }
}
