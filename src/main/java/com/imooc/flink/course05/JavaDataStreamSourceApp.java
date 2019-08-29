package com.imooc.flink.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JavaDataStreamSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //nonParallelSourceFunction(env);
        //parallelSourceFunction(env);
        richParallelSourceFunction(env);
        env.execute("JavaDataStreamSourceApp");
    }

    private static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        /**
         * 自定义一个NonParallelSourceFunction
         */
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelSourceFunction());
        data.print().setParallelism(1);
    }
    private static void parallelSourceFunction(StreamExecutionEnvironment env) {
        /**
         * 自定义一个ParallelSourceFunction
         */
        DataStreamSource<Long> data = env.addSource(new JavaCustomParallelSourceFunction())
                .setParallelism(2);
        data.print();
    }
    private static void richParallelSourceFunction(StreamExecutionEnvironment env) {
        /**
         * 自定义一个RichParallelSourceFunction
         */
        DataStreamSource<Long> data = env.addSource(new JavaCustomRichParallelSourceFunction())
                .setParallelism(2);
        data.print();
    }
}
