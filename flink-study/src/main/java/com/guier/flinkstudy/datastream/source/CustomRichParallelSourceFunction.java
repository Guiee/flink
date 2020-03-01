package com.guier.flinkstudy.datastream.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.TimeUnit;

public class CustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {
    private long count = 0L;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning && count < 1000) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(count);
                count += 1;
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
