package com.imooc.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromCollection(env)
    textFile(env)
  }
  def fromCollection(env:ExecutionEnvironment):Unit={
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }
  def textFile(env:ExecutionEnvironment):Unit={
//    val filePath = "E:\\IdeaProjects\\imooc-workspace\\flink-train\\src\\test\\data.txt"
//    env.readTextFile(filePath)
//      .print()
      val filePath = "E:\\IdeaProjects\\imooc-workspace\\flink-train\\src\\test\\inputs"
          env.readTextFile(filePath)
            .print()
  }
}
