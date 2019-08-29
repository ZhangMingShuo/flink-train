package com.imooc.flink.course05;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;
/**
 自定义Sink：本课程使用Java语言来实现，Scala语言作为作业自己来实现
 */
@Builder
@ToString
@Data
public class Student {
    private int id;
    private String name;
    private int age;
}
