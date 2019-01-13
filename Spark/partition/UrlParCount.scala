package com.simon.spark.partition

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object UrlParCount {

  def main(args: Array[String]): Unit = {

    // 1. 创建SparkContext
    val conf:SparkConf = new SparkConf().setAppName("UrlParCount").setMaster("local[2]")
    val sc:SparkContext = new SparkContext(conf)

    // 2. 加载数据
    val rdd1 = sc.textFile("/Users/simon/Hadoop/Spark/log-in").map(line => {
      val s:Array[String] = line.split("\t")

      (s(1), 1)
    })

    // 3. 聚合
    val rdd2 = rdd1.reduceByKey(_ + _)

    // 4. 自定义格式
    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      val xHost = host.split("[.]")(0)

      (xHost, (url, t._2))
    })

    // 5. 加入自定义分区
    val xueyuan:Array[String] = rdd3.map(_._1).distinct.collect()
    val xueYuanPartitioner:XueYuanPartitioner = new XueYuanPartitioner(xueyuan)

    // 6. 加入分区规则
    val rdd4:RDD[(String, (String, Int))] = rdd3.partitionBy(xueYuanPartitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.iterator
    })

    rdd4.saveAsTextFile("/Users/simon/Hadoop/Spark/log-out")

    sc.stop()
  }
}

class XueYuanPartitioner(xy:Array[String]) extends Partitioner {

  // 自定义规则 学院 分区号
  val rules:mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  var number = 0

  // 遍历学院
  for(i <- xy) {
    // 学院与分区号对应
    rules += (i -> number)
    // 分区号递增
    number += 1
  }

  override def numPartitions: Int = xy.length

  override def getPartition(key: Any): Int = {
    rules.getOrElse(key.toString, 0)
  }
}
