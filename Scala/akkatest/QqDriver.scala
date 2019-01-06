package com.simon.akkatest

import akka.actor.{ActorSystem, Props}

object QqDriver {

  // 1. 创建ActorSystem, 用ActorSystem创建Actor
  private val qqFactory = ActorSystem("QqFactory")

  // 2. Actor发送、接收消息通过ActorRef代理
  private val sRef = qqFactory.actorOf(Props[SimonActor], "Simon")

  // 接收Simon发送的消息
  private val yRef = qqFactory.actorOf(Props(new SyyActor(sRef)), "syy")

  def main(args:Array[String]):Unit = {
    // 1. Simon自己给自己发消息
    sRef ! "I love U"

    // 2. syy给Simon发消息
    yRef ! "Hello, I'm Simon"
  }
}
