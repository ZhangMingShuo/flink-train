package com.imooc.flink.course04;
import org.apache.flink.api.java.ExecutionEnvironment;
import java.util.*;
public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        textFile(env);
        //fromCollection(env);
    }

    /**
     * 从集合创建dataset
     * @param env
     * @throws Exception
     */
    public static void fromCollection(ExecutionEnvironment env ) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i <= 10 ; i++) {
            list.add(i);
        }
        env.fromCollection(list).print();
    }

    /**
     * 从文件或者文件夹创建dataset
     * @param env
     * @throws Exception
     */
    public static void textFile(ExecutionEnvironment env) throws Exception {
        env.readTextFile("E:\\IdeaProjects\\imooc-workspace\\flink-train\\src\\test\\data.txt")
                .print();
        System.out.println("env = [" + env + "]");
    }
}