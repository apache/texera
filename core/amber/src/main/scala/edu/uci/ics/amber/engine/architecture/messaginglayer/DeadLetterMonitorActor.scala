package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, DeadLetter}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkSenderActor.{MessageBecomesDeadLetter, NetworkMessage}

class DeadLetterMonitorActor extends Actor {
  override def receive: Receive = {
    case d: DeadLetter =>
      d.message match{
        case networkMessage: NetworkMessage =>
          d.sender ! MessageBecomesDeadLetter(networkMessage)
        case other =>
          // skip for now
      }
    case _ =>
  }
}
