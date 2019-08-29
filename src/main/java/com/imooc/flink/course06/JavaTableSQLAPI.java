package com.imooc.flink.course06;

import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class JavaTableSQLAPI {
    public static void main(String[] args)throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);//1.8
        //BatchTableEnvironment.getTableEnvironment(env);// 1.7
        DataSource<Sales> csv = env
                .readCsvFile("src/test/inputs/sales.csv")
                .ignoreFirstLine()
                .pojoType(Sales.class,"transactionId","customerId","itemId","amountPaid");
        //csv.print();
        Table sales = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales",sales);
        Table resultTable = tableEnv.sqlQuery("select customerId,sum(amountPaid) money from sales group by customerId");
        DataSet<Row> result = tableEnv.toDataSet(resultTable, Row.class);
        result.print();
    }
    @Data
    @ToString
    private static class Sales{
        private String transactionId;
        private String customerId;
        private String itemId;
        private double amountPaid;
    }
}
