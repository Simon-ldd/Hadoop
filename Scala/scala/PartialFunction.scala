package com.simon.scala

object PartialFunction {

  def func(str:String):Int = {
    if (str.equals("simon"))
      18
    else
      0
  }

  def func1:PartialFunction[String, Int] = {
    case "simon" => 18

    case _ => 0
  }

  def main(args: Array[String]): Unit = {
    println(func("simon"))
    println(func1("Simon"))
    println(func1("simon"))
  }
}
