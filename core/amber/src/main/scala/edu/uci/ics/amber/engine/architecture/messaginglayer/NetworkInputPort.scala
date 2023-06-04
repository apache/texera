package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkAck
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class NetworkInputPort[T](
    val actorId: ActorVirtualIdentity,
    val handler: (ActorVirtualIdentity, T) => Unit
) extends AmberLogging {

  private val idToOrderingEnforcers =
    new mutable.AnyRefMap[ActorVirtualIdentity, OrderingEnforcer[T]]()

  def handleMessage(
      sender: ActorRef,
      senderCredits: Int,
      messageID: Long,
      from: ActorVirtualIdentity,
      sequenceNumber: Long,
      payload: T
  ): Unit = {
    sender ! NetworkAck(messageID, Some(senderCredits))
    val entry = idToOrderingEnforcers.getOrElseUpdate(from, new OrderingEnforcer[T]())
    if (entry.isDuplicated(sequenceNumber)) { // discard duplicate
      logger.info(
        s"receive a duplicated message: $payload from $sender with seqNum = $sequenceNumber while current = ${entry.current}"
      )
    } else if (entry.isAhead(sequenceNumber)) {
      logger.info(
        s"receive a message that is ahead of the current, stashing: $payload from $sender with seqNum = $sequenceNumber"
      )
      entry.stash(sequenceNumber, payload)
    } else {
      entry.enforceFIFO(payload).foreach(v => handler.apply(from, v))
    }
  }

  def overwriteFIFOState(fifoState: Map[ActorVirtualIdentity, Long]): Unit = {
    fifoState.foreach {
      case (identity, l) =>
        val entry = idToOrderingEnforcers.getOrElseUpdate(identity, new OrderingEnforcer[T]())
        entry.setCurrent(l)
    }
  }

  def getStashedMessageCount(): Long = {
    if (idToOrderingEnforcers.isEmpty) { return 0 }
    idToOrderingEnforcers.values.map(ordEnforcer => ordEnforcer.ofoMap.size).sum
  }

}
