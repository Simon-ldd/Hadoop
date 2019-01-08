package com.simon.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    // 1. 设置参数 setAppName设置程序名， setMaster本地测试设置线程数 *多个
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[2]")

    // 2. 创建Spark执行程序的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3. 加载数据 并且处理
    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
    // 保存文件
      .saveAsTextFile(args(1))

    // 4. 关闭资源
    sc.stop()
  }
}
