package com.simon.scala

import java.util.Date

object PartParam extends App {

  def log(date:Date, message:String) = {
    println(s"$date, $message")
  }

  val date = new Date();
  val logMessage = log(date, _:String)

  log(date, "hellosimon")
  logMessage("simon")
}
