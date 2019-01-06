package com.simon.akkatest

import akka.actor.{Actor, ActorRef}

class SyyActor(val h:ActorRef) extends Actor {
  override def receive: Receive = {
    case "Hello, I'm Simon" => {
      // 发送消息给SimonActor
      h ! "I love U"
    }
  }
}
