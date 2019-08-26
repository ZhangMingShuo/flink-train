package com.imooc.flink.course04

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromCollection(env)
    //textFile(env)
    //csvFile(env)
    //csvFileToPojo(env)
    //readRecursiveFiles(env)
    //readCompressionFiles(env)
    mapPartitionFunction(env)
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
  case class MyNameAgeClass(name:String,age:Int)
  /**
    * 读取csv文件的内容,并读取第一列和第三列的值name，job
    * @param env 执行环境
    */
  def csvFile(env:ExecutionEnvironment):Unit = {
      import org.apache.flink.api.scala._
      val filePath = "E:\\IdeaProjects\\imooc-workspace\\flink-train\\src\\test\\inputs\\people.csv"
//      env.readCsvFile[(String,String)](filePath,includedFields=Array(0,2),ignoreFirstLine=true)
//      .print()

    //case class MyNameAgeClass(name:String,age:Int)  自定义case class写在函数内部会报错，应该写在函数外

    env.readCsvFile[MyNameAgeClass](filePath,ignoreFirstLine = true ,includedFields = Array(0,1))
      .print()
  }

  def csvFileToPojo(executionEnvironment: ExecutionEnvironment):Unit={
    import org.apache.flink.api.scala._
    val filePath = "E:\\IdeaProjects\\imooc-workspace\\flink-train\\src\\test\\inputs\\people.csv"
    executionEnvironment.readCsvFile[Person](filePath,ignoreFirstLine = true,pojoFields = Array("name","age","job"))
      .print()
  }

  /**
    * 递归地读取文件夹中的文件的数据
    * @param executionEnvironment 执行环境
    */
  def readRecursiveFiles(executionEnvironment: ExecutionEnvironment):Unit={
    val filePath="src/test/nested"
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration",true)//默认的配置
    executionEnvironment.readTextFile(filePath).withParameters(parameters)
      .print()
  }

  //flink1.8好像只支持读取gz文件
  def readCompressionFiles(executionEnvironment: ExecutionEnvironment):Unit={
    val filePath="src/test/inputs/compression/gz"
    executionEnvironment.readTextFile(filePath)
      .print()
  }

  /**
    * mapPartition,把一百个学生存入数据库 data.mapPartition {in => in map{(_,1)}}
    * 为了提高与数据库传递数据的性能，使用mapPartitionFunction
    */
  def mapPartitionFunction(executionEnvironment: ExecutionEnvironment):Unit={
    val students = new ListBuffer[String]
    for(i<-1 to 100)
      students.append("student: "+i)
    val data = executionEnvironment.fromCollection(students)
        .setParallelism(4)
//    data.map(x=>{
//      val connection = DBUtils.getConnection()
//      println(connection+"...")
//      //TODO... 保存数据到DB
//      DBUtils.returnConnection(connection)
//    }).print()

    data.mapPartition(x => {
      val connection = DBUtils.getConnection()
      println(connection+"....")
      DBUtils.returnConnection(connection)
      x
    }).print()
  }
}