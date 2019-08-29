package com.imooc.flink.course05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * 需求：socket发送数据过来，把string类型转成对象，然后把Java对象保存到MySQL数据库
 * 1,PK,20
 * 2,Jepson,18
 */
public class JavaCustomSinkToMysql {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<Student> studentStream = source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] splits = value.split(",");
                return Student.builder().id(Integer.parseInt(splits[0])).name(splits[1]).age(Integer.parseInt(splits[2])).build();
                //return value;
            }
        });
        studentStream.addSink(new SinkToMySQL());
        env.execute("JavaCustomSinkToMysql");
    }
}
