package com.imooc.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment

object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromCollection(env)
    //textFile(env)
    csvFile(env)
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

  /**
    * 读取csv文件的内容,并读取第一列和第三列的值name，job
    * @param env
    */
  def csvFile(env:ExecutionEnvironment):Unit = {
      import org.apache.flink.api.scala._
      val filePath = "E:\\IdeaProjects\\imooc-workspace\\flink-train\\src\\test\\inputs\\people.csv"
      env.readCsvFile[(String,String)](filePath,includedFields=Array(0,2),ignoreFirstLine=true)
      .print()
  }
}