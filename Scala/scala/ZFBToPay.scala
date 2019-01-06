package com.simon.scala

object ZFBToPay {

  var moneny = 1000

  // 吃一次反花50元
  def eat() = {
    moneny = moneny - 50
  }

  // 余额
  def balance():Int = {
    eat()
    moneny
  }

  def printMoneny(x:Int) = {
    for (a <- 1 to 5) {
      println(f"balance: $x")
    }
  }

  def main(args: Array[String]): Unit = {
    eat()
    balance()
    printMoneny(balance())
  }
}
