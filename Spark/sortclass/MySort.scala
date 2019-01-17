package com.simon.spark.sortclass

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MySort {

  def main(args: Array[String]): Unit = {

    // 1. spark程序入口
    val conf: SparkConf = new SparkConf().setAppName("Mysort").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    // 2. 创建数组
    val girl: Array[String] = Array("reba,18,80", "mimi,22,100", "liya,30,89", "jingtian,18,78")

    // 3. 转换RDD
    val grdd1: RDD[String] = sc.parallelize(girl)

    // 4. 切分数据
    val grdd2: RDD[Girl] = grdd1.map(line => {
      val fields: Array[String] = line.split(",")

      new Girl(fields(0), fields(1).toInt, fields(2).toInt)
    })

    val sorted: RDD[Girl] = grdd2.sortBy(s => s)
    val r: Array[Girl] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

class Girl(val name: String, val age: Int, val weight: Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int  = {
    // 年龄相同， 体重重的往前排
    if (this.age == that.age) {
      that.weight - this.weight
    } else {
      this.age - that.age
    }
  }

  override def toString: String = s"名字: $name, 年龄: $age, 体重: $weight"
}
