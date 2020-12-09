package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorLogging, Stash}
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.messaginglayer.MessagingManager
import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.DataMessage

class WorkflowActor extends Actor with ActorLogging with Stash {

  lazy val input: FIFOAccessPort = wire[FIFOAccessPort]
  lazy val messagingManager: MessagingManager = wire[MessagingManager]

  override def receive: Receive = {
    case msg: DataMessage =>
      messagingManager.receiveMessage(msg, sender)
    case other =>
  }
}
