package com.imooc.flink.course08;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class JavaKafkaConnectorProducerApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从socket接收数据，通过Flink，将数据Sink到Kafka
        DataStream<String> stream = env.socketTextStream("localhost", 9999);
        String topic = "ptest";
        Properties properties = new Properties();
        //使用hadoop000 必须要求idea的hostname和ip的映射关系正确
        properties.setProperty("bootstrap.servers","192.168.88.199:9092");
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>("192.168.88.199:9092", topic, new SimpleStringSchema());
        stream.addSink(myProducer);
        env.execute("KafkaConnectorProducerApp");
    }
}
