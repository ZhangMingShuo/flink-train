package com.imooc.flink.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

object KafkaConnectorProducerApp {
  def main(args: Array[String]): Unit = {
    val env  = StreamExecutionEnvironment.getExecutionEnvironment
    //从socket接收数据，通过Flink，将数据Sink到Kafka
    val data = env.socketTextStream("localhost",9999)
    val topic = "ptest"
    val properties = new Properties()
    //使用hadoop000 必须要求idea的hostname和ip的映射关系正确
    properties.setProperty("bootstrap.servers","192.168.88.199:9092")
    val kafkaSink = new FlinkKafkaProducer[String](topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      properties
    )
    data.addSink(kafkaSink)
    env.execute("KafkaConnectorProducerApp")
  }
}