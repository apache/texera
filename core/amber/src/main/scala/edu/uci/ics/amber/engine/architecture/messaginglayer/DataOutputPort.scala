package edu.uci.ics.amber.engine.architecture.messaginglayer

import java.util.concurrent.atomic.AtomicLong
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowDataMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkSenderActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

/** This class handles the assignment of sequence numbers to data
  * The internal logic can send data messages to other actor without knowing
  * where the actor is and without determining the sequence number.
  */
class DataOutputPort(selfID: ActorVirtualIdentity, networkSenderActor: NetworkSenderActorRef) {

  private val idToSequenceNums = new mutable.AnyRefMap[ActorVirtualIdentity, AtomicLong]()

  def sendTo(to: ActorVirtualIdentity, payload: DataPayload): Unit = {
    var receiverId = to
    if (to == ActorVirtualIdentity.Self) {
      // selfID and VirtualIdentity.Self should be one key. Although it should never happen
      // that data-message is sent by an actor to itself. But this check is here to avoid any bugs.
      receiverId = selfID
    }
    val msg = WorkflowDataMessage(
      selfID,
      idToSequenceNums.getOrElseUpdate(to, new AtomicLong()).getAndIncrement(),
      payload
    )
    networkSenderActor ! SendRequest(to, msg)
  }

}
