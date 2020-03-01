package com.guier.flinkstudy.datastream.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class SourceFunctionApp implements SourceFunction<Long> {
    private long count = 0L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning && count < 1000) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(count);
                count++;
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

    }

    // @Override
    // public void run(SourceContext ctx) throws Exception {
    //     while (isRunning && count < 1000) {
    //         // this synchronized block ensures that state checkpointing,
    //         // internal state updates and emission of elements are an atomic operation
    //         synchronized (ctx.getCheckpointLock()) {
    //             ctx.collect(count);
    //             count++;
    //             TimeUnit.SECONDS.sleep(1);
    //         }
    //     }
    // }
    //
    // @Override
    // public void cancel() {
    //     isRunning = false;
    // }
}
