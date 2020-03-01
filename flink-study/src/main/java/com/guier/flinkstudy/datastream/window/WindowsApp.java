package com.guier.flinkstudy.datastream.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

// https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#tumbling-windows
public class WindowsApp {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


    @Test
    public void tumblingWindows() throws Exception {
        DataStreamSource<String> date = env.socketTextStream("localhost", 9999);
        // 默认是处理时间 ProcessingTime，摄入时间，事件时间
        // env.setStreamTimeCharacteristic();
        date.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.toLowerCase().split(",");
                for (String s : split) {
                    if (s.length() > 0) {
                        out.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();

        env.execute("WindowsApp");
    }

    @Test
    public void slidingWindows() throws Exception {
        DataStreamSource<String> date = env.socketTextStream("localhost", 9999);
        date.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.toLowerCase().split(",");
                for (String s : split) {
                    if (s.length() > 0) {
                        out.collect(new Tuple2<>(s, 1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(9), Time.seconds(1)).sum(1).print().setParallelism(1);

        env.execute("WindowsApp");
    }

    // https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#reducefunction
    @Test
    public void reducefunction() throws Exception {
        DataStreamSource<String> date = env.socketTextStream("localhost", 9999);
        date.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                String[] split = value.toLowerCase().split(",");
                for (String s : split) {
                    if (s.length() > 0) {
                        out.collect(new Tuple2<>(1,Integer.parseInt(s)));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                System.out.println("value1 = [" + value1 + "], value2 = [" + value2 + "]");
                return new Tuple2<>(1, value1.f1 + value2.f1);
            }
        }).print().setParallelism(1);

        env.execute("WindowsApp");
    }

    @Test
    public void processWindowFunction() throws Exception {
        DataStreamSource<String> date = env.socketTextStream("localhost", 9999);
        date.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                String[] split = value.toLowerCase().split(",");
                for (String s : split) {
                    if (s.length() > 0) {
                        out.collect(new Tuple2<>(1, Integer.parseInt(s)));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).process(new ProcessWindowFunction<Tuple2<Integer,Integer>, Object, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<Object> out) throws Exception {
                long count = 0;
                System.out.println("~~~~~~~~~~~");
                for (Tuple2<Integer, Integer> in: elements) {
                    count++;
                }
                out.collect("Window: " + context.window() + "count: " + count);
            }
        }).print().setParallelism(1);

        env.execute("WindowsApp");
    }


}
