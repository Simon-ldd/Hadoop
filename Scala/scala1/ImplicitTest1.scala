package com.simon.scala1

object ImplicitTest1 {
  implicit def double2Int(d:Double) = {
    d.toInt
  }

  def main(args:Array[String]):Unit = {
    val a:Int = 18.08
    println(a)
  }
}
