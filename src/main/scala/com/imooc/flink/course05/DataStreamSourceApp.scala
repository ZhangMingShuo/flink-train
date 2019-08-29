package com.imooc.flink.course05

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object DataStreamSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //parallelSourceFunction(env)
    richParallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }

  def richParallelSourceFunction(env: StreamExecutionEnvironment):Unit = {
    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    data.print()
  }

  def parallelSourceFunction(env: StreamExecutionEnvironment):Unit = {
    val data = env.addSource(new CustomRichParallelSourceFunction).setParallelism(2)
    data.print()
  }
}
