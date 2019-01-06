package com.simon.scalatest.classtest

private[this] class Person(var name:String, age:Int) {
  var high:Int = _

  def this(name:String, age:Int, high:Int) {
    this(name, high)
    this.high = high
  }
}

//object Test2 extends App{
//  val p = new Person("simon", 18)
//  println(p.name)
//}
