package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.WorkerStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{
  ControlInvocation,
  ReturnPayload,
  noReplyNeeded
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}

/** Motivation of having a separate module to handle control messages as RPCs:
  * In the old design, every control message and its response are handled by
  * message passing. That means developers need to manually send response back
  * and write proper handlers on the sender side.
  * Writing control messages becomes tedious if we use this way.
  *
  * So we want to implement rpc model on top of message passing.
  * rpc (request-response)
  * remote.callFunctionX().then(response => {
  * })
  * user-api: promise
  *
  * goal: request-response model with multiplexing
  * client: initiate request
  * (web browser, actor that invoke control command)
  * server: handle request, return response
  * (web server, actor that handles control command)
  */
object AsyncRPCServer {

  trait ControlCommand[T]

  final case class CommandCompleted()

}

class AsyncRPCServer(controlOutputPort: ControlOutputPort, logger: WorkflowLogger) {

  // all handlers
  protected var handlers: PartialFunction[(ControlCommand[_], ActorVirtualIdentity), Future[_]] =
    PartialFunction.empty

  // note that register handler allows multiple handlers for a control message and uses the latest handler.
  def registerHandler(
      newHandler: PartialFunction[(ControlCommand[_], ActorVirtualIdentity), Future[_]]
  ): Unit = {
    handlers =
      newHandler orElse handlers

  }

  def receive(control: ControlInvocation, senderID: ActorVirtualIdentity): Unit = {
    try {
      execute((control.command, senderID)) match {
        case f: Future[_] =>
          // user's code returns a future
          // the result should be returned after the future is resolved.
          f.onSuccess { ret =>
            returnResult(senderID, control.commandID, ret)
          }
          f.onFailure { err =>
            returnResult(senderID, control.commandID, err)
          }
      }
    } catch {
      case e: Throwable =>
        // if error occurs, return it to the sender.
        returnResult(senderID, control.commandID, e)
        throw e
    }
  }

  @inline
  private def returnResult(sender: ActorVirtualIdentity, id: Long, ret: Any): Unit = {
    if (noReplyNeeded(id)) {
      return
    }
    controlOutputPort.sendTo(sender, ReturnPayload(id, ret))
  }

  def execute(cmd: (ControlCommand[_], ActorVirtualIdentity)): Future[_] = {
    handlers(cmd)
  }

  def logControlInvocation(call: ControlInvocation, sender: VirtualIdentity): Unit = {
    if (call.commandID == AsyncRPCClient.IgnoreReplyAndDoNotLog) {
      return
    }
    if (call.command.isInstanceOf[QueryStatistics]) {
      return
    }
    logger.logInfo(
      s"receive command: ${call.command} from ${sender.toString} (controlID: ${call.commandID})"
    )
  }

}
