package com.simon.spark.sorttmp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

      // 元组输出
      (fields(0), fields(1).toInt, fields(2).toInt)
    })

    // 5. 模式匹配方式进行排序
    val sorted: RDD[(String, Int, Int)] = grdd2.sortBy(s => Girl(s._1, s._2, s._3))
    val r: Array[(String, Int, Int)] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

case class Girl(val name: String, val age: Int, val weight: Int) extends Ordered[Girl] {
  override def compare(that: Girl): Int = {
    if (this.age == that.age) {
      that.weight - this.weight
    } else {
      this.age - that.age
    }
  }

  override def toString: String = s"名字: $name, 年龄: $age, 体重: $weight"
}
