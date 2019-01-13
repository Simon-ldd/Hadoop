package com.simon.spark.urltop

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UrlGroupCount {

  def main(args: Array[String]): Unit = {

    // 1. 创建SparkContext
    val conf:SparkConf = new SparkConf().setAppName("UrlGroupCount").setMaster("local[2]")
    val sc:SparkContext = new SparkContext(conf)

    // 2. 加载数据
    val rdd1:RDD[String] = sc.textFile("/Users/simon/Hadoop/Spark/log-in")

    // 3. 切分
    val rdd2:RDD[(String, Int)] = rdd1.map(line => {
      val s:Array[String] = line.split("\t")

      (s(1), 1)
    })

    // 4. 求出总的访问量
    val rdd3:RDD[(String, Int)] = rdd2.reduceByKey(_ + _)

    // 5. 取出学院
    val rdd4:RDD[(String, String, Int)] = rdd3.map(x => {
      // 拿到url
      val url:String = x._1

      // java中拿到主机名
      val host:String = new URL(url).getHost.split("[.]")(0)
      (host, url, x._2)
    })

    // 6. 按照学院进行分组
    val rdd5:RDD[(String, List[(String, String, Int)])] = rdd4.groupBy(_._1).mapValues(it => {
      // 倒序
      it.toList.sortBy(_._3).reverse.take(1)
    })

    // 7. 遍历输出
    rdd5.foreach(x => {
      println("学院为: " + x._1 + ", 访问量第一的为: " + x._2(0)._2 + ", " + x._2(0)._3)
    })

    sc.stop()
  }
}
