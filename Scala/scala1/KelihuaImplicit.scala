package com.simon.scala1

object KelihuaImplicit {
  // 柯理化
  def sum(a:Int)(implicit b:Int) = (a + b)

  def main(args: Array[String]): Unit = {
    // 定义隐式值
    implicit val a = 9
    println(sum(1))
  }
}
