package com.guier.flinkstudy.datastream;

import com.guier.flinkstudy.datastream.source.CustomRichParallelSourceFunction;
import com.guier.flinkstudy.datastream.source.ParallelSourceFunctionApp;
import com.guier.flinkstudy.datastream.source.SourceFunctionApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class DataStreamSourceApp {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    @Test
    public void noParallelSourceFunction() throws Exception {
        DataStreamSource<Long> data = env.addSource(new SourceFunctionApp()).setParallelism(1);//设置为1或者不设置
        data.print().setParallelism(1);
        env.execute("JavaDataStreanSourceApp");
    }

    @Test
    public void parallelSourceFunction() throws Exception {
        DataStreamSource<Long> data = env.addSource(new ParallelSourceFunctionApp()).setParallelism(3);
        data.print();
        env.execute("JavaDataStreanSourceApp");
    }

    @Test
    public void richParallelSourceFunction() throws Exception {
        DataStreamSource<Long> data = env.addSource(new CustomRichParallelSourceFunction()).setParallelism(3);
        data.print();
        env.execute("JavaDataStreanSourceApp");
    }
    /**
     * $nc -lk 9999
     */
    @Test
    public void socketFunction() throws Exception {
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        data.print().setParallelism(1);
        env.execute("JavaDataStreanSourceApp");
    }
}
