package com.simon.scala1

object ImplicitTest {
  def sleep(implicit how:String = "做梦") = {
    println(how)
  }

  def eat(implicit name:String): Unit = {
    println(name)
  }

  def main(args: Array[String]): Unit = {
    sleep
    implicit val name = "pig"
    eat
  }
}
