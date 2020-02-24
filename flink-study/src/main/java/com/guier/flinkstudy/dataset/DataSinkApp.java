package com.guier.flinkstudy.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.junit.Test;

import java.util.Arrays;

public class DataSinkApp {
    public static void main(String[] args) throws Exception {

    }

    @Test
    public void sink() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Integer> data = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
        data.print();
        String filePath = "file:///Users/apuuu/data/sink_out";
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
        // 如果不设置并行度，输出文件名为sink_out
        env.execute("DataSinkApp");
    }

    @Test
    public void parallelismSink() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度，输出文件为1 2 3
        DataSource<Integer> data = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9)).setParallelism(3);
        data.print();
        String filePath = "file:///Users/apuuu/data/sink_out";
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
        // 如果不设置并行度，输出文件名为sink_out
        env.execute("DataSinkApp");
    }
}
