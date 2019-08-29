package com.imooc.flink.course05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaDataStreamTransformationApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        filterFunction(env);
        unionFunction(env);
        env. execute("JavaDataStreamTrans formationApp");
    }

    private static void unionFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long>data1 = env.addSource(new JavaCustomNonParallelSourceFunction());
        DataStreamSource<Long>data2 = env.addSource(new JavaCustomNonParallelSourceFunction());
        data1.union(data2).print().setParallelism(1);
    }

    private static void filterFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> data=env.addSource(new JavaCustomNonParallelSourceFunction());
        data. map(new MapFunction<Long, Long>() {
            public Long map(Long value) {
                System.out.println("value = [" + value + "]");
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) {
                return value%2 == 0;
            }
        }).print().setParallelism(1);
    }
}
