package com.simon.spark.implicitsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ImplicitRules {
  // 定义隐式规则
  implicit object OrderingGirl extends Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.age == y.age) {
        y.weight - x.weight
      } else {
        x.age - y.age
      }
    }
  }
}

object MySort {
  def main(args: Array[String]): Unit = {
    // 1. 程序入口
    val conf: SparkConf = new SparkConf().setAppName("MySort").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 2. 创建数组
    val girl: Array[String] = Array("reba,18,80", "mimi,22,100", "liya,30,89", "jingtian,18,78")

    // 3. 转换RDD
    val grdd1: RDD[String] = sc.parallelize(girl)

    // 4. 切分数据
    val grdd2: RDD[(String, Int, Int)] = grdd1.map(line => {
      val fields: Array[String] = line.split(",")

      (fields(0), fields(1).toInt, fields(2).toInt)
    })

    // 导入隐式
    import ImplicitRules.OrderingGirl
    val sorted: RDD[(String, Int, Int)] = grdd2.sortBy(s => Girl(s._1, s._2, s._3))
    val r: Array[(String, Int, Int)] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

case class Girl(val name: String, val age: Int, val weight: Int)
