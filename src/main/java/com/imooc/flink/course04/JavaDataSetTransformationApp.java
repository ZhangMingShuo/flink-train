package com.imooc.flink.course04;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
public class JavaDataSetTransformationApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        mapPartitionFunction(env);
//        firstFunction(env);
          flatMapFunction(env);
//        distinctFunction(env);
        //joinFunction(env);
        crossFunction(env);
    }

    private static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String>info1 = new ArrayList<>();
        info1.add("曼联");
        info1.add("曼城");
        List<String>info2 = new ArrayList<>();
        info2.add("3");
        info2.add("1");
        info2.add("0");
        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<String> data2 = env.fromCollection(info2);
        data1.cross(data2).print();
    }

    private static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String>info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");
        DataSource<String> data = env.fromCollection(info);
        data.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[]splits = value.split(",");
            for (String split :
                    splits) {
                out.collect(split);
            }
        }).distinct()
                .print();
    }

    private static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String>info = new ArrayList<>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");
        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) {
                String[]splits = value.split(",");
                for (String split:splits
                     ) {
                    out.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                return new Tuple2<>(value,1);
            }
        }).groupBy(0)
                .sum(1)
                .print();

    }

    private static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer,String>>info = new ArrayList<>();
        info.add(new Tuple2<>(1,"Hadoop"));
        info.add(new Tuple2<>(1,"Spark"));
        info.add(new Tuple2<>(1,"Flink"));
        info.add(new Tuple2<>(2,"Java"));
        info.add(new Tuple2<>(2,"Spring Boot"));
        info.add(new Tuple2<>(3,"Linux"));
        info.add(new Tuple2<>(4,"VUE"));
        DataSource<Tuple2<Integer, String>> tuple2DataSource = env.fromCollection(info);
        tuple2DataSource
                .groupBy(0).sortGroup(1, Order.ASCENDING)
                .first(2)
                .print();
    }

    private static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10 ; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            /**
             * The mapping method. Takes an element from the input data set and transforms
             * it into exactly one element.
             *
             * @param value The input value.
             * @return The transformed value
             */
            @Override
            public Integer map(Integer value) {
                return value + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) {
                return value > 5;
            }
        }).print();
    }

    /**
     * mapPartitionFunction
     * @param env 执行环境
     */
    private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String>list = new ArrayList<>();
        for (int i = 1; i < 101; i++) {
            list.add("student"+i);
        }
        DataSource<String>data = env.fromCollection(list).setParallelism(6);
//        data.map(new MapFunction<String,String>(){
//            public String map(String value) throws Exception {
//                String connection = DBUtils.getConnection();
//                System.out.println("connection = [" + connection + "]");
//                DBUtils.returnConnection(connection);
//                return value;
//            }
//        }).print();
        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) {
                String connection = DBUtils.getConnection();
                System.out.println("connection = [" + connection + "]");
                DBUtils.returnConnection(connection);
            }
        }).print();
    }
}