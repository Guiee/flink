package com.guier.flinkstudy.datastream;

import com.guier.flinkstudy.datastream.source.ParallelSourceFunctionApp;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DataStreamOperators {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void splitSelectFunction() throws Exception {
        DataStreamSource<Long> data = env.addSource(new ParallelSourceFunctionApp()).setParallelism(3);
        SplitStream<Long> splitStream = data.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });
        // splitStream.select("even").print();
        splitStream.select("odd").print();

        env.execute("DataStreamOperatorsApp");
    }

    @Test
    public void sinkFunction() throws Exception {
        DataStreamSource<Student> data = null;
        data.addSink(new SinkToMySQL());
        env.execute("DataStreamSink");
    }

}
