package com.imooc.flink.course05

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CustomRichParallelSourceFunction extends RichParallelSourceFunction[Long]{
  var isRunning = true
  var count = 1L
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(isRunning){
      ctx.collect(count)//构造数据格式
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit ={
    isRunning=false
  }
}
