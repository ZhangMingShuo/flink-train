package com.imooc.flink.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop","spark","flink","pyspark","storm")
//    data.map(new RichMapFunction[String,Long] {
//      var counter = 0l
//      override def map(value: String): Long = {
//        counter = counter + 1
//        println("counter :"+counter)
//        counter
//      }
//    }).print() //并行度为1时可以计数

    val info = data.map(new RichMapFunction[String,String]() {
      //定义计数器
      val counter = new LongCounter()
      //open
      override def open(parameters: configuration.Configuration): Unit = {
        getRuntimeContext.addAccumulator("ele-counts-scala",counter)
      }
      override def map(value: String): String = {
        counter.add(1)
        value
      }
    })
    info.writeAsText("src/test/sink-out2",WriteMode.OVERWRITE).setParallelism(5)
    val jobResult = env.execute("CounterApp")
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    println(num)
  }
}
