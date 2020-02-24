package com.guier.flinkstudy.dataset;

import com.guier.flinkstudy.DBUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class DataSetFunctionApp {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) {


    }


    /**
     * // MapFunction that adds two integer values
     * public class IntAdder implements MapFunction<Tuple2<Integer, Integer>, Integer> {
     *
     * @Override public Integer map(Tuple2<Integer, Integer> in) {
     * return in.f0 + in.f1;
     * }
     * }
     * <p>
     * // [...]
     * DataSet<Tuple2<Integer, Integer>> intPairs = // [...]
     * DataSet<Integer> intSums = intPairs.map(new IntAdder());
     */
    @Test
    public void mapFunction() throws Exception {


        DataSource<Integer> data = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer a) throws Exception {
                return a + 1;
            }
        }).print();
        //这么写也可以
        // data.map(a -> a + 1).print();
    }

    // 过滤
    @Test
    public void filterFunction() throws Exception {


        DataSource<Integer> data = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer e) throws Exception {
                return e + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer > 5 && integer % 2 == 0;
            }
        }).print();

        // data.map(e -> e + 1).filter(e -> e > 5 && e % 2 == 0).print();
    }

    @Test
    // 常用于存到数据库，避免重复消耗资源
    public void mapPartitionFunction() throws Exception {

        ArrayList<String> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add("student: " + i);
        }
        DataSource<String> data = env.fromCollection(list).setParallelism(5);
        // data.map(new MapFunction<String, String>() {
        //     @Override
        //     public String map(String s) throws Exception {
        //         int connection = DBUtils.getConnection();
        //         DBUtils.returnConnection(connection);
        //         return s;
        //     }
        // }).print();
        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                int connection = DBUtils.getConnection();
                DBUtils.returnConnection(connection);
                for (String s : iterable) {
                    collector.collect(s);
                }
            }
        }).print();
    }

    // 去topX
    @Test
    public void firstFunction() throws Exception {
        ArrayList<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, "java"));
        list.add(new Tuple2<>(1, "scala"));
        list.add(new Tuple2<>(1, "hadoop"));
        list.add(new Tuple2<>(2, "vue"));
        list.add(new Tuple2<>(2, "js"));
        list.add(new Tuple2<>(3, "语文"));
        list.add(new Tuple2<>(3, "数学"));
        list.add(new Tuple2<>(3, "英语"));
        DataSource<Tuple2<Integer, String>> data = env.fromCollection(list);
        data.first(3).print();
        System.out.println();
        data.groupBy(0).first(2).print();
        System.out.println();
        data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();

    }

    // 从一个产生一个或多个元素
    @Test
    public void flatMapFunction() throws Exception {
        ArrayList<String> list = new ArrayList<>();
        list.add("java,scala");
        list.add("map,list");
        list.add("java,map");
        list.add("a,c");

        DataSource<String> data = env.fromCollection(list);
        // data.first(3).print();
        // System.out.println();
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).groupBy(0).sum(1).print();

    }

    // 去重
    @Test
    public void distinctFunction() throws Exception {
        ArrayList<String> list = new ArrayList<>();
        list.add("java,scala");
        list.add("map,list");
        list.add("java,map");
        list.add("a,c");

        DataSource<String> data = env.fromCollection(list);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        }).distinct().print();

    }

    @Test
    public void joinFunction() throws Exception {
        ArrayList<Tuple2<Integer, String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1, "java"));
        list1.add(new Tuple2<>(2, "vue"));
        list1.add(new Tuple2<>(3, "数学"));
        list1.add(new Tuple2<>(4, "英语"));

        ArrayList<Tuple2<Integer, String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1, "jj"));
        list2.add(new Tuple2<>(2, "uu"));
        list2.add(new Tuple2<>(3, "yy"));
        list2.add(new Tuple2<>(5, "ss"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(list1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(list2);

        // inner join
        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first.f0 == second.f0) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
                return null;
            }
        }).print();
        // left outer join
        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (second == null) {
                    return new Tuple3<>(first.f0, first.f1, "*");
                }
                if (first.f0 == second.f0) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
                return null;
            }
        }).print();
        // right outer join
        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<>(second.f0, "*", second.f1);
                }
                if (first.f0 == second.f0) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
                return null;
            }
        }).print();
        // full join
        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<>(second.f0, "*", second.f1);
                }
                if (second == null) {
                    return new Tuple3<>(first.f0, first.f1, "*");
                }
                if (first.f0 == second.f0) {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
                return null;
            }
        }).print();

    }

    // 笛卡尔积
    @Test
    public void crossFunction() throws Exception {
        ArrayList<String> list1 = new ArrayList<>();
        ArrayList<String> list2 = new ArrayList<>();
        list1.add("MM");
        list1.add("DD");
        list2.add("1");
        list2.add("2");
        list2.add("3");
        DataSource<String> data1 = env.fromCollection(list1);
        DataSource<String> data2 = env.fromCollection(list2);
        data1.cross(data2).print();
    }
}
