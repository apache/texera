package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{ActorRef}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkAck, NetworkMessageGeneric}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

class NetworkInputPort[T](
    val logger: WorkflowLogger,
    val handler: (VirtualIdentity, T) => Unit,
) {

  private val idToOrderingEnforcers =
    new mutable.AnyRefMap[VirtualIdentity, OrderingEnforcer[T]]()

  def handleMessage(sender: ActorRef, message: NetworkMessageGeneric[T]): Unit = {
    sender ! NetworkAck(message.messageID)
    val internalMessage = message.internalMessage
    OrderingEnforcer.reorderMessage[T](
      idToOrderingEnforcers,
      internalMessage.from,
      internalMessage.sequenceNumber,
      internalMessage.payload
    ) match {
      case Some(iterable) =>
        iterable.foreach(v => handler.apply(internalMessage.from, v))
      case None =>
        // discard duplicate
        logger.logInfo(s"receive duplicated: ${internalMessage.payload} from ${internalMessage.from}")
    }
  }

}
