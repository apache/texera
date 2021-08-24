package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkSenderActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/** This class handles the assignment of sequence numbers to controls
  * The internal logic can send control messages to other actor without knowing
  * where the actor is and without determining the sequence number.
  */
class ControlOutputPort(selfID: ActorVirtualIdentity, networkSenderActor: NetworkSenderActorRef) {
  private val idToSequenceNums = new mutable.AnyRefMap[ActorVirtualIdentity, AtomicLong]()

  def sendTo(to: ActorVirtualIdentity, payload: ControlPayload): Unit = {
    var receiverId = to
    if (to == SELF) {
      // selfID and VirtualIdentity.SELF should be one key
      receiverId = selfID
    }
    val seqNum = idToSequenceNums.getOrElseUpdate(receiverId, new AtomicLong()).getAndIncrement()
    val msg = WorkflowControlMessage(selfID, seqNum, payload)
    networkSenderActor ! SendRequest(to, msg)
  }

}
