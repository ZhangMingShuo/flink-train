package com.imooc.flink.course05

import java.{lang, util}

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object DataStreamTransformationApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //filterFunction(env)
    //unionFunction(env)
    splitSelectFunction(env)
    env.execute("DataStreamTransformationApp")
  }

  def filterFunction(env: StreamExecutionEnvironment):Unit = {
    val data = env.addSource(new JavaCustomNonParallelSourceFunction)
    data.map(x=>{
      println("received: "+x)
      x})
      .filter(_%2==0).print().setParallelism(1)
  }
  def unionFunction(env: StreamExecutionEnvironment):Unit = {
    val data1 = env.addSource(new JavaCustomNonParallelSourceFunction)
    val data2 = env.addSource(new JavaCustomNonParallelSourceFunction)
    data1.union(data2).print().setParallelism(1)
  }


  def splitSelectFunction(env: StreamExecutionEnvironment) :Unit={
    val data = env.addSource(new JavaCustomNonParallelSourceFunction)
    val splits = data.split(new OutputSelector[lang.Long] {
      override def select(value: lang.Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if(value % 2 == 0){
          list.add("even")
        }else{
          list.add("odd")
        }
        list
      }
    })
    //splits.select("odd").print().setParallelism(1)
    //splits.select("even").print().setParallelism(1)
    splits.select("even","odd").print().setParallelism(1)
  }
}
