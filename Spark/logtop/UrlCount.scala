package com.simon.spark.logtop

import org.apache.spark.{SparkConf, SparkContext}

object UrlCount {

  def main(args: Array[String]): Unit = {

    // 1. 加载数据
    val conf:SparkConf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc:SparkContext = new SparkContext(conf)

    // 载入数据
    val rdd1 = sc.textFile("/Users/simon/Hadoop/Spark/log-in")

    // 2. 对数据进行计算
    val rdd2 = rdd1.map(line => {
      val s:Array[String] = line.split("\t")

      // 标注为出现1次
      (s(1), 1)
    })

    // 3. 将相同的网址进行累加求和
    val rdd3 = rdd2.reduceByKey(_ + _)

    // 4. 排序 取出前三
    val rdd4 = rdd3.sortBy(_._2, false).take(3)

    // 5. 遍历打印
    rdd4.foreach(x => {
      println("网址为: " + x._1 + " 访问量: " + x._2)
    })

    sc.stop()
  }
}
