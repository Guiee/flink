package com.guier.flinkstudy.dataset;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;

/**
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/batch/
 * <p>
 * File-based:
 * <p>
 * readTextFile(path) / TextInputFormat - Reads files line wise and returns them as Strings.
 * <p>
 * readTextFileWithValue(path) / TextValueInputFormat - Reads files line wise and returns them as StringValues. StringValues are mutable strings.
 * <p>
 * readCsvFile(path) / CsvInputFormat - Parses files of comma (or another char) delimited fields. Returns a DataSet of tuples or POJOs. Supports the basic java types and their Value counterparts as field types.
 * <p>
 * readFileOfPrimitives(path, Class) / PrimitiveInputFormat - Parses files of new-line (or another char sequence) delimited primitive data types such as String or Integer.
 * <p>
 * readFileOfPrimitives(path, delimiter, Class) / PrimitiveInputFormat - Parses files of new-line (or another char sequence) delimited primitive data types such as String or Integer using the given delimiter.
 * <p>
 * Collection-based:
 * <p>
 * fromCollection(Collection) - Creates a data set from a Java.util.Collection. All elements in the collection must be of the same type.
 * <p>
 * fromCollection(Iterator, Class) - Creates a data set from an iterator. The class specifies the data type of the elements returned by the iterator.
 * <p>
 * fromElements(T ...) - Creates a data set from the given sequence of objects. All objects must be of the same type.
 * <p>
 * fromParallelCollection(SplittableIterator, Class) - Creates a data set from an iterator, in parallel. The class specifies the data type of the elements returned by the iterator.
 * <p>
 * generateSequence(from, to) - Generates the sequence of numbers in the given interval, in parallel.
 * <p>
 * Generic:
 * <p>
 * readFile(inputFormat, path) / FileInputFormat - Accepts a file input format.
 * <p>
 * createInput(inputFormat) / InputFormat - Accepts a generic input format.
 */
public class DataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        createFromCollection(env);
//         createFromTextFile(env);
        createRecursiveTraversal(env);
    }

    public static void createFromCollection(ExecutionEnvironment env) throws Exception {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }

    public static void createFromTextFile(ExecutionEnvironment env) throws Exception {
        // 读文件或者读文件夹
        // String filepath = "file:///Users/apuuu/data";
        String filepath = "file:///Users/apuuu/data/input.txt";

        env.readTextFile(filepath).print();
    }

    public static void createFromCvsFile(ExecutionEnvironment env) throws Exception {
        // 读文件或者读文件夹
        // String filepath = "file:///Users/apuuu/data";
        String filepath = "file:///Users/apuuu/data/input.txt";

    }

    public static void createRecursiveTraversal(ExecutionEnvironment env) throws Exception {
        // 读文件或者读文件夹
        // String filepath = "file:///Users/apuuu/data";
        String filepath = "file:///Users/apuuu/data";
        Configuration parameters = new Configuration();

        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);

        // pass the configuration to the data source
        env.readTextFile(filepath)
                .withParameters(parameters).print();
    }


    /**
     * ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
     *
     * // read text file from local files system
     * DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");
     *
     * // read text file from an HDFS running at nnHost:nnPort
     * DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");
     *
     * // read a CSV file with three fields
     * DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
     * 	                       .types(Integer.class, String.class, Double.class);
     *
     * // read a CSV file with five fields, taking only two of them
     * DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
     *                                .includeFields("10010")  // take the first and the fourth field
     * 	                       .types(String.class, Double.class);
     *
     * // read a CSV file with three fields into a POJO (Person.class) with corresponding fields
     * DataSet<Person>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
     *                          .pojoType(Person.class, "name", "age", "zipcode");
     *
     * // read a file from the specified path of type SequenceFileInputFormat
     * DataSet<Tuple2<IntWritable, Text>> tuples =
     *  env.createInput(HadoopInputs.readSequenceFile(IntWritable.class, Text.class, "hdfs://nnHost:nnPort/path/to/file"));
     *
     * // creates a set from some given elements
     * DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");
     *
     * // generate a number sequence
     * DataSet<Long> numbers = env.generateSequence(1, 10000000);
     *
     * // Read data from a relational database using the JDBC input format
     * DataSet<Tuple2<String, Integer> dbData =
     *     env.createInput(
     *       JDBCInputFormat.buildJDBCInputFormat()
     *                      .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
     *                      .setDBUrl("jdbc:derby:memory:persons")
     *                      .setQuery("select name, age from persons")
     *                      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
     *                      .finish()
     *     );
     */
}
