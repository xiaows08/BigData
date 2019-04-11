package cn.xiaows.akka.demo

import akka.actor.{Actor, ActorRef}

class PingActor(fg: ActorRef) extends Actor {
	override def receive: Receive = {
		case _ => println("oooo")
	}
}
