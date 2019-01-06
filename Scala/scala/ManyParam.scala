package com.simon.scala

object ManyParam {

  def sum(ints:Int*):Int = {
    var sum = 0
    for(v <- ints) {
      sum += v
    }

    sum
  }

  def setName(params:Any*):Any = {
    return params
  }

  def main(args: Array[String]): Unit = {
    println(sum(1, 2, 3, 4))
    println(sum(1, 2))
    println(setName("simon", 18, 188))
  }
}
