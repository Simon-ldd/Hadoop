package com.simon.scala

object GaoJie {

  def getPerson(h:Int => String, f:Int):String = {
    h(f)
  }

  def Person(x:Int) = "我是" + x.toString + "岁很帅的simon"

  def main(args: Array[String]): Unit = {
    println(getPerson(Person, 18))
  }
}
