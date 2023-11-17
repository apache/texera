package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, DeadLetter}
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  MessageBecomesDeadLetter,
  NetworkMessage
}

class DeadLetterMonitorActor extends Actor {
  override def receive: Receive = {
    case d: DeadLetter =>
      d.message match {
        case msg: NetworkMessage =>
          // d.sender is the NetworkSenderActor
          d.sender ! MessageBecomesDeadLetter(msg)
        case other =>
        // skip for now
      }
    case _ =>
  }
}
