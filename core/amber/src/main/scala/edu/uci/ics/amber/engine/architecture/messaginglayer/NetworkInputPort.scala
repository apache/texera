package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorRef
import com.typesafe.scalalogging.LazyLogging
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

    OrderingEnforcer.reorderMessage[T](
      idToOrderingEnforcers,
      from,
      sequenceNumber,
      payload
    ) match {
      case Some(iterable) =>
        iterable.foreach(v => handler.apply(from, v))
      case None =>
        // discard duplicate
        logger.info(s"receive duplicated: $payload from $from")
    }
  }

  def getStashedMessageCount(): Long = {
    if (idToOrderingEnforcers.size == 0) { return 0 }
    idToOrderingEnforcers.values.map(ordEnforcer => ordEnforcer.ofoMap.size).sum
  }

}
