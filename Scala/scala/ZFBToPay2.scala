package com.simon.scala

object ZFBToPay2 {

  var moneny = 1000

  def eat() = {
    moneny = moneny - 50
  }

  def balance():Int = {
    eat()
    moneny
  }

  // 此时余额减了5次
  def printMoneny(x: => Int) = {
    for (a <- 1 to 5) {
      println(f"balance: $x")
    }
  }

  def main(args: Array[String]): Unit = {
    printMoneny(balance)
  }
}