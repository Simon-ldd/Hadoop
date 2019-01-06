package com.simon.akkatest

import akka.actor.Actor

class SimonActor extends Actor {

  override def receive: Receive = {
    case "Hello, I'm syy" => {
      println("Hello, I'm Simon")
    }
    case "I love U" => {
      println("Simon love syy")
    }
  }
}
