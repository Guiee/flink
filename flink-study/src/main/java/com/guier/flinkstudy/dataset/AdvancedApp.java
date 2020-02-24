package com.guier.flinkstudy.dataset;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;

public class AdvancedApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromCollection(Arrays.asList("Tokyo", "Canada", "United States", "China", "Australia"));
        MapOperator<String, String> operator = data.map(new RichMapFunction<String, String>() {
            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("ele-counts", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        }).setParallelism(3);
        String filePath = "file:///Users/apuuu/data/dataAdvancedSinkAppOut";
        operator.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
        // 如果不设置并行度，输出文件名为sink_out
        JobExecutionResult jobExecutionResult = env.execute("DataAdvancedSinkApp");
        long count = jobExecutionResult.getAccumulatorResult("ele-counts");
        System.out.println(count);
    }
}
