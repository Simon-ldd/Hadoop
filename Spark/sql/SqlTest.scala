package com.simon.spark.sql

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SqlTest {
  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession
    val sparkSession: SparkSession = new spark.sql.SparkSession.Builder().appName("SqlTest")
      .master("local[2]")
      .getOrCreate()

    // 2. 创建RDD
    val dataRdd: RDD[String] = sparkSession.sparkContext.textFile("/Users/simon/Hadoop/Spark/Sql/user")

    // 3. 切分数据
    val splitRdd: RDD[Array[String]] = dataRdd.map(_.split(","))

    // 4. 封装数据
    val rowRdd: RDD[Row] = splitRdd.map(x => {
      // 封装一行数据
      Row(x(0).toInt, x(1).toString, x(2).toInt)
    })

    // 5. 创建Schema(描述DataFrame信息)
    val schema: StructType = StructType(List(
      // 列名、类型、可否为空
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    // 6. 创建DataFrame
    val userDF: DataFrame = sparkSession.createDataFrame(rowRdd, schema)

    // 7. 注册表
    userDF.registerTempTable("user_t")

    // 8. 写Sql
    val useSql: DataFrame = sparkSession.sql("select * from user_t order by age")

    // 9. 查看结果
    useSql.show()

    // 10. 释放资源
    sparkSession.stop()
  }
}
