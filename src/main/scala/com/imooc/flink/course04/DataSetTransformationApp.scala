package com.imooc.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //mapFunction(env)
    //filterFunction(env)
    firstFunction(env)
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

  /**
    * 定义FilterFunction,查找/过滤掉不符合条件的数据
    */
  def filterFunction(executionEnvironment: ExecutionEnvironment):Unit={
//    val data = executionEnvironment.fromCollection(List(1,2,3,4,5,6,7,8,9,10))
//    data.map(_ + 1).filter(_>5).print()
    //链式编程，jQuery
    executionEnvironment.fromCollection(List(1,2,3,4,5,6,7,8,9,10))
      .map(_ + 1)
      .filter(_ > 5)
      .print()
  }

  def firstFunction(executionEnvironment: ExecutionEnvironment)={
    val info = ListBuffer[(Int,String)]()
    info.append((1,"Hadoop"))
    info.append((1,"Spark"))
    info.append((1,"Flink"))
    info.append((2,"Java"))
    info.append((2,"Spring Boot"))
    info.append((3,"Linux"))
    info.append((4,"VUE"))
    val data = executionEnvironment.fromCollection(info)
    //取前4个
    data.first(4).print()
    /**
      * (1,Hadoop)
      * (1,Spark)
      * (1,Flink)
      * (2,Java)
      */
    data.groupBy(0).first(2).print()
    /**
      * (3,Linux)
      * (1,Hadoop)
      * (1,Spark)
      * (2,Java)
      * (2,Spring Boot)
      * (4,VUE)
      */
    data.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print()
    /**
      * (3,Linux)
      * (1,Flink)
      * (1,Hadoop)
      * (2,Java)
      * (2,Spring Boot)
      * (4,VUE)
      */
    data.groupBy(0).sortGroup(1,Order.DESCENDING).first(2).print()

  }
}