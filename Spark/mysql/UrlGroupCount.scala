package com.simon.spark.mysql

import java.net.URL
import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UrlGroupCount {

  def main(args: Array[String]): Unit = {

    // 1. 创建Spark程序入口
    val conf: SparkConf = new SparkConf().setAppName("UrlGroupCount").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 2. 加载数据
    val rdd1: RDD[String] = sc.textFile("/Users/simon/Hadoop/Spark/log-in")

    // 3. 将数据切分
    val rdd2: RDD[(String, Int)] = rdd1.map(line => {
      val s: Array[String] = line.split("\t")

      (s(1), 1)
    })

    // 4. 累加求和
    val rdd3: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)

    // 5. 取出分组的学院
    val rdd4: RDD[(String, (String, Int))] = rdd3.map(x => {
      val url = x._1
      val host = new URL(url).getHost.split("[.]")(0)

      (host, (x._1, x._2))
    })

    // 6. 根据学院分组
    val rdd5: RDD[(String, List[(String, (String, Int))])] = rdd4.groupBy(_._1).mapValues(it => {
      // 根据访问量排序 倒序
      it.toList.sortBy(_._2).reverse.take(1)
    })


    // 7. 把结果存到Mysql中
    rdd5.foreach(x => {
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/urlcount?charatorEncoding=utf-8", "root", "Wyhhi@yy19941120")

      // 把spark结果插入到mysql中， 采用问号防注入
      val sql = "INSERT INTO url_data (xueyuan, number_one) VALUES (?, ?)"
      // 执行sql
      val statement = conn.prepareStatement(sql)
      statement.setString(1, x._1)
      statement.setString(2, x._2(0)._2._2.toString)
      statement.executeUpdate()
      statement.close()
      conn.close()
    })

    // 8. 关闭资源
    sc.stop()
  }

}
