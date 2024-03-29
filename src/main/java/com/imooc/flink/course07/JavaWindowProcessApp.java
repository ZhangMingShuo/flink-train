package com.imooc.flink.course07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class JavaWindowProcessApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);
        text.flatMap(new FlatMapFunction<String, Tuple2<Integer,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                String[]tokens = value.toLowerCase().split(",");
                for (String token :
                        tokens) {
                    if (token.length() > 0)
                        out.collect(new Tuple2<Integer, Integer>(1,Integer.parseInt(token)));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, Object, Tuple, TimeWindow>() {
                    /**
                     * Evaluates the window and outputs none or several elements.
                     *
                     * @param tuple    The key for which this window is evaluated.
                     * @param context  The context in which the window is being evaluated.
                     * @param elements The elements in the window being evaluated.
                     * @param out      A collector for emitting elements.
                     * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
                     */
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<Object> out) throws Exception {
                        System.out.println("~~~~~~~~~~~~");
                        long count = 0;
                        for (Tuple2<Integer, Integer>in:elements){
                            count++;
                        }
                        out.collect("Window: "+context.window()+"count:"+count);
                    }
                })
                .print()
                .setParallelism(1);
        env.execute("JavaWindowsApp");
    }
}
