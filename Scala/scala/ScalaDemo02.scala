package com.simon.scala

object ScalaDemo02 {
  def main(args: Array[String]): Unit = {
    println(m1(1, 9))
    println(m2(2, 8))
  }

  // 加法
  def m1(a:Int, b:Int): Int = {
    a + b
  }

  def m2(a:Int, b:Int): Int = a * b
}
