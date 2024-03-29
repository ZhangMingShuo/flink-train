package com.imooc.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetTransformationApp {

  def flatMapFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")
    val data = env.fromCollection(info)
//    data.flatMap(_.split(","))
//      .print()
    /**
      * word count
      */
    data.flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }

  def distinctFunction(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")
    val data = env.fromCollection(info)
    data.flatMap(_.split(","))
      .distinct()
      .print()
  }

  def leftOuterJoinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int,String)]()
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))
    val info2 = ListBuffer[(Int,String)]()
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.leftOuterJoin(data2).where(0).equalTo(0)
      .apply((first,second)=>{
        if(second==null)
          (first._1,first._2,"-")
        else
          (first._1,first._2,second._2)
      }).print()
  }

  def rightOutFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int,String)]()
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))
    val info2 = ListBuffer[(Int,String)]()
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{//注意连接的列分别是是两个表的哪一列属性
      if(first==null)//一定记住哪一个为空 就不打印哪一个，而打印另外一个
        (second._1,"-",second._2)
      else
        (first._1,first._2,second._2)
    }).print()
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //mapFunction(env)
    //filterFunction(env)
    //firstFunction(env)
    //useFirst(env)
    //flatMapFunction(env)
    //distinctFunction(env)
    //joinFunction(env)
    //leftOuterJoinFunction(env)
    //rightOutFunction(env)
    //fullOuterJoin(env)
    crossFunction(env)
  }

  def crossFunction(env: ExecutionEnvironment): Unit = {
    val info1 = List("曼联","曼城")//两支球队
    val info2 = List(3,1,0)//得分情况
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.cross(data2).print() //笛卡尔积 求组合情况
  }

  def fullOuterJoin(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int,String)]()
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))
    val info2 = ListBuffer[(Int,String)]()
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
      if(first==null)
        (second._1,"-",second._2)
      else if(second==null)
        (first._1,first._2,"-")
      else (first._1,first._2,second._2)
    })
      .print()
  }

  def joinFunction(env: ExecutionEnvironment): Unit = {
    val info1 = ListBuffer[(Int,String)]()
    info1.append((1,"PK哥"))
    info1.append((2,"J哥"))
    info1.append((3,"小队长"))
    info1.append((4,"猪头呼"))
    val info2 = ListBuffer[(Int,String)]()
    info2.append((1,"北京"))
    info2.append((2,"上海"))
    info2.append((3,"成都"))
    info2.append((5,"杭州"))
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.join(data2).where(0).equalTo(0)
      .apply((first,second)=>
        (first._1,first._2,second._2)
      ).print()
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

  def firstFunction(executionEnvironment: ExecutionEnvironment):Unit={
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
    //data.first(4).print()
    /**
      * (1,Hadoop)
      * (1,Spark)
      * (1,Flink)
      * (2,Java)
      */
    //data.groupBy(0).first(2).print()
    /**
      * (3,Linux)
      * (1,Hadoop)
      * (1,Spark)
      * (2,Java)
      * (2,Spring Boot)
      * (4,VUE)
      */
    //data.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print()
    /**
      * (3,Linux)
      * (1,Flink)
      * (1,Hadoop)
      * (2,Java)
      * (2,Spring Boot)
      * (4,VUE)
      */
    data.groupBy(0)
      .sortGroup(1,Order.DESCENDING)
      .first(2)
      .print()

  }

  def useFirst(executionEnvironment: ExecutionEnvironment):Unit={
    val info = ListBuffer[(Int,String)]()
    info.append((1,"Hadoop"))
    info.append((1,"Spark"))
    info.append((1,"Selenium"))
    info.append((1,"Flink"))
    info.append((2,"Java"))
    info.append((2,"Spring Boot"))
    info.append((3,"Linux"))
    info.append((4,"VUE"))
    executionEnvironment.fromCollection(info)
      .groupBy(0).sortGroup(1,Order.ASCENDING)
      .first(3)
      .print()
  }
}