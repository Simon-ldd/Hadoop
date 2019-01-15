package com.simon.spark.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDDDemo {

  def main(args: Array[String]): Unit = {

    // 1. 创建Spark程序入口
    val conf: SparkConf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 匿名函数
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/urlcount?charatorEncoding=utf-8", "root", "Wyhhi@yy19941120")
    }

    // 查询数据
    val jdbcRDD: JdbcRDD[(Int, String, String)] = new JdbcRDD(
      // 指定SparkContext
      sc,
      connection,
      "SELECT * FROM url_data where uid >= ? AND uid <= ?",
      // 2个任务并行
      1, 4, 2,
      r => {
        val uid = r.getInt(1)
        val xueyuan = r.getString(2)
        val number_one = r.getString(3)
        (uid, xueyuan, number_one)
      }
    )

    val jrdd = jdbcRDD.collect()
    println(jrdd.toBuffer)
    sc.stop()
  }

}
