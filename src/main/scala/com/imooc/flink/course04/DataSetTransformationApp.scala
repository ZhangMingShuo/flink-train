package com.imooc.flink.course04

import org.apache.flink.api.scala._

object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    mapFunction(env)
  }

  def mapFunction(executionEnvironment: ExecutionEnvironment):Unit={
    val data = executionEnvironment.fromCollection(List(1,2,3,4,5,6,7,8,9,10))
    //    data.map((x:Int)=>x+2)
    //      .print()
    //data.map并不会改变data中的值

    /*
      data.map((x:Int)=>x+2)
      data.print()
     */

    /*
    data.map(x=>x+1)
      .print()
     */

    data.map(_+1)
      .print()
  }
}