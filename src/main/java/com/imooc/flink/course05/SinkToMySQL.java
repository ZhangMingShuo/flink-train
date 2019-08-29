package com.imooc.flink.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMySQL extends RichSinkFunction<Student> {
    private Connection connection;
    private PreparedStatement ps;
    private Connection getConnection(){
        Connection conn = null;
        try{
            String driver = "com.mysql.cj.jdbc.Driver";
            Class.forName(driver);
            String url = "jdbc:mysql://localhost:3306/mooc?&useSSL=false&serverTimezone=UTC";
            String username = "root";
            String passworld = "123456";
            conn = DriverManager.getConnection(url,username,passworld);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sql = "insert into student(id, name,age) values(?, ?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setInt(3, value.getAge());
        ps.executeUpdate();
    }
}
