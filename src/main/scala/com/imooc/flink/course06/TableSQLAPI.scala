package com.imooc.flink.course06

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

object TableSQLAPI {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //val tableEnv = TableEnvironment.getTableEnvironment(env) 1.7
    val tableEnv = BatchTableEnvironment.create(env)//1.8
    val filePath = "src/test/inputs/sales.csv"
    //拿到DataSet ，转换成Table
    val csv = env.readCsvFile[SalesLog](filePath,ignoreFirstLine = true)
    val salesTable = tableEnv.fromDataSet(csv)
    tableEnv.registerTable("sales", salesTable)//注册成一张表table
    val resultTable = tableEnv.sqlQuery("select customerId,sum(amountPaid) money from sales group by customerId")
    tableEnv.toDataSet[Row](resultTable).print()
  }
  case class SalesLog(transactionId:String,
                      customerId:String,
                      itemId:String,
                      amountPaid:Double)
}
