package com.imooc.flink.course08
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
object KafkaConnectorConsumerApp {
  def main(args: Array[String]): Unit = {
    val env  = StreamExecutionEnvironment.getExecutionEnvironment
    val topic = "ptest"
    val properties = new Properties()
    //使用hadoop000 必须要求idea的hostname和ip的映射关系正确
    properties.setProperty("bootstrap.servers","192.168.88.199:9092")
    properties.setProperty("group.id","ptest")
    val data = env.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),properties))
    data.print()
    env.execute("KafkaConnectorConsumerApp")
  }
}