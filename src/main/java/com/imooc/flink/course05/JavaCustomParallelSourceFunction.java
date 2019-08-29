package com.imooc.flink.course05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class JavaCustomParallelSourceFunction implements ParallelSourceFunction<Long> {
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
}
