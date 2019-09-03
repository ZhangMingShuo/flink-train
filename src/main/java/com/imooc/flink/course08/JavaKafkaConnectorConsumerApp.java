package com.imooc.flink.course08;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class JavaKafkaConnectorConsumerApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "ptest";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.88.199:9092");
        properties.setProperty("group.id","ptest");
        DataStream<String> topic1 = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        topic1.print();
        env.execute("JavaKafkaConnectorConsumerApp");
    }
}
