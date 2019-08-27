package com.imooc.flink.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class JavaDataSetSinkApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer>list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        String filePath ="src/test/sink-out";
        DataSource<Integer> data = env.fromCollection(list);
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(3);
        env.execute("JavaDataSetSinkApp");
    }
}
