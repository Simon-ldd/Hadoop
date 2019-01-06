package com.simon.akkatest

import akka.actor.{Actor, ActorSystem, Props}

object CallMe {

  // 1. 创建ActorSystem, 用ActorSystem创建Actor
  private val acFactory = ActorSystem("AcFactory")

  // 2. Actor发送、接收消息通过ActorRef代理
  private val callRef = acFactory.actorOf(Props[CallMe], "CallMe")

  def main(args:Array[String]):Unit = {
    // 3. 发送消息
    callRef ! "Simon很帅"
    callRef ! "Simon不帅"
    callRef ! "stop"
  }
}

class CallMe extends Actor {

  override def receive:Receive = {
    case "Simon很帅" => println("你是对的")
    case "Simon不帅" => println("你是错的")
    case "stop" => {
      // 关闭自己的代理
      context.stop(self)

      // 关闭ActorSystem
      context.system.terminate()
    }
  }
}
