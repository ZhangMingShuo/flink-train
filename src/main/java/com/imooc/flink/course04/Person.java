package com.imooc.flink.course04;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Person {
    private String name;
    private Integer age;
    private String job;
}
