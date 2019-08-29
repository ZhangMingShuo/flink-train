package com.imooc.flink.course05

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object DataStreamTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    filterFunction(env)
    env.execute("DataStreamTransformationApp")
  }

  def filterFunction(env: StreamExecutionEnvironment):Unit = {
    val data = env.addSource(new JavaCustomNonParallelSourceFunction)
    data.map(x=>{
      println("received: "+x)
      x})
      .filter(_%2==0).print().setParallelism(1)
  }

}
