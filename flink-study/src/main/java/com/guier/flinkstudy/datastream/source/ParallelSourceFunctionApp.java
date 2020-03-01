package com.guier.flinkstudy.datastream.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.TimeUnit;

public class ParallelSourceFunctionApp implements ParallelSourceFunction<Long> {
    private long count = 0L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning && count < 1000) {
            // this synchronized block ensures that state checkpointing,
            // internal state updates and emission of elements are an atomic operation
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(count);
                count += 1;
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
