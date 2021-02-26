package edu.uci.ics.amber.engine.architecture.messaginglayer

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.worker.WorkerStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.collection.mutable

object ControlInputPort {
  final case class WorkflowControlMessage(
      from: VirtualIdentity,
      sequenceNumber: Long,
      payload: ControlPayload
  ) extends WorkflowMessage
}

class ControlInputPort(
    logger: WorkflowLogger,
    asyncRPCClient: AsyncRPCClient,
    asyncRPCServer: AsyncRPCServer
) {

  private val idToOrderingEnforcers =
    new mutable.AnyRefMap[VirtualIdentity, OrderingEnforcer[ControlPayload]]()

  def handleControlMessage(msg: WorkflowControlMessage): Unit = {
    OrderingEnforcer.reorderMessage(
      idToOrderingEnforcers,
      msg.from,
      msg.sequenceNumber,
      msg.payload
    ) match {
      case Some(iterable) =>
        iterable.foreach {
          case call: ControlInvocation =>
            processControlInvocation(call, msg.from)
          case ret: ReturnPayload =>
            processReturnPayload(ret, msg.from)
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

  @inline
  def processControlInvocation(invocation: ControlInvocation, from: VirtualIdentity): Unit = {
    assert(from.isInstanceOf[ActorVirtualIdentity])
    asyncRPCServer.logControlInvocation(invocation, from)
    asyncRPCServer.receive(invocation, from.asInstanceOf[ActorVirtualIdentity])
  }

  @inline
  def processReturnPayload(ret: ReturnPayload, from: VirtualIdentity): Unit = {
    asyncRPCClient.logControlReply(ret, from)
    asyncRPCClient.fulfillPromise(ret)
  }

}
