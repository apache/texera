package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkAck
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

import scala.collection.mutable

class NetworkInputPort[T](
    val logger: WorkflowLogger,
    val handler: (VirtualIdentity, T) => Unit
) {

  private val idToOrderingEnforcers =
    new mutable.AnyRefMap[VirtualIdentity, OrderingEnforcer[T]]()

  def handleMessage(
      sender: ActorRef,
      messageID: Long,
      from: VirtualIdentity,
      sequenceNumber: Long,
      payload: T
  ): Unit = {
    sender ! NetworkAck(messageID)

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
        logger.logInfo(s"receive duplicated: ${payload} from ${from}")
    }
  }

}
