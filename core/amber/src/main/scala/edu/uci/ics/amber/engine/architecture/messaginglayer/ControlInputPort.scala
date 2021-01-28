package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.neo.{ControlPayload, WorkflowMessage}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCServer, AsyncRPCClient}
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.collection.mutable

object ControlInputPort {
  final case class WorkflowControlMessage(
      from: VirtualIdentity,
      sequenceNumber: Long,
      payload: ControlPayload
  ) extends WorkflowMessage
}

class ControlInputPort(asyncRPCClient: AsyncRPCClient, asyncRPCServer: AsyncRPCServer) {

  protected val logger: WorkflowLogger = WorkflowLogger("ControlInputPort")

  private val idToOrderingEnforcers =
    new mutable.AnyRefMap[VirtualIdentity, OrderingEnforcer[ControlPayload]]()

  def handleControlMessage(msg: WorkflowControlMessage): Unit = {
//    println(
//      s"RECEIVED in Control Input Port from ${msg.from}: ${msg.sequenceNumber}: ${msg.payload}"
//    )
    OrderingEnforcer.reorderMessage(
      idToOrderingEnforcers,
      msg.from,
      msg.sequenceNumber,
      msg.payload
    ) match {
      case Some(iterable) =>
        iterable.foreach {
          case call: ControlInvocation =>
            assert(msg.from.isInstanceOf[ActorVirtualIdentity])
            asyncRPCServer.receive(call, msg.from.asInstanceOf[ActorVirtualIdentity])
          case ret: ReturnPayload =>
            asyncRPCClient.fulfillPromise(ret)
          case other =>
            logger.logError(
              WorkflowRuntimeError(
                s"unhandled control message: $other",
                "ControlInputPort",
                Map.empty
              )
            )
        }
      case None =>
        // discard duplicate
        logger.logInfo(s"receive duplicated: ${msg.payload} from ${msg.from}")
    }
  }
}
