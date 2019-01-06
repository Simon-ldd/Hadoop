package com.simon.scala

object DefaultParam {

  def sum(a:Int = 3, b:Int = 7):Int = {
    a + b
  }

  def main(args: Array[String]): Unit = {
    println(sum(1, 2))
    println(sum())
  }
}
