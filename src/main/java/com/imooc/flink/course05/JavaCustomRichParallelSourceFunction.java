package com.imooc.flink.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 继承RichParallelSourceFunction这个抽象类
 */
public class JavaCustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {
    boolean isRunning = true;
    long count = 1;
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning){
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    public void cancel() {
        isRunning = false;
    }

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    public void close() throws Exception {
        super.close();
    }
}
