package edu.uci.ics.amber.engine.architecture.messaginglayer

import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkSenderActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.neo.ControlPayload
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable

/** This class handles the assignment of sequence numbers to controls
  * The internal logic can send control messages to other actor without knowing
  * where the actor is and without determining the sequence number.
  */
class ControlOutputPort(selfID: ActorVirtualIdentity, networkSenderActor: NetworkSenderActorRef) {

  protected val logger: WorkflowLogger = WorkflowLogger("ControlOutputPort")

  private val idToSequenceNums = new mutable.AnyRefMap[ActorVirtualIdentity, AtomicLong]()

  def sendTo(to: ActorVirtualIdentity, payload: ControlPayload): Unit = {
    val seqNum = idToSequenceNums.getOrElseUpdate(to, new AtomicLong()).getAndIncrement()
    val msg = WorkflowControlMessage(selfID, seqNum, payload)
    logger.logInfo(s"send $msg to $to")
    networkSenderActor ! SendRequest(to, msg)
  }

}
