package com.simon.scala.array

object MapTest {
  def main(args: Array[String]): Unit = {
    val arr = Array(1, 2, 3)

    // 调用map方法（映射关系， 函数）
    val arr1 = arr.map(x => x * 10)

    println(arr.toBuffer)
    println(arr1.toBuffer)
  }
}
