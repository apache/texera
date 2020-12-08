package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorLogging, Stash}
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.messaging.MessagingManager

class WorkflowActor extends Actor with ActorLogging with Stash {

  lazy val messagingManager: MessagingManager = wire[MessagingManager]

  override def receive: Receive = {
    case msg =>

  }
}
