package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{
  ControlInvocation,
  ReturnPayload
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand



/** Motivation of having a separate module to handle control messages as RPCs:
  * message passing is a low level abstraction -- manually handle response
  *
  * rpc (request-response)
  * remote.callFunctionX().then(response => {
  * })
  * user-api: promise -- straightforward
  *
  * so we want to implement rpc model on top of message passing
  * goal: request-response model with multiplexing
  * client: initiate request
  * (web browser, actor that invoke control command)
  * server: handle request, return response
  * (web server, actor that handles control command)
  *
  */
object AsyncRPCServer {

  trait ControlCommand[T]
}

class AsyncRPCServer(controlOutputPort: ControlOutputPort) {

  // all handlers
  protected var handlers: PartialFunction[ControlCommand[_], Any] = PartialFunction.empty

  // note that register handler allows multiple handlers for a control message and uses the latest handler.
  def registerHandler(newHandler: PartialFunction[ControlCommand[_], Any]): Unit = {
    handlers =
      newHandler orElse handlers

  }

  def receive(control: ControlInvocation, senderID: ActorVirtualIdentity): Unit = {
    try {
      handlers(control.command) match {
        case f: Future[_] =>
          // user's code returns a future
          // the result should be returned after the future is resolved.
          f.onSuccess { ret =>
            returnResult(senderID, control.commandID, ret)
          }
          f.onFailure { err =>
            returnResult(senderID, control.commandID, err)
          }
        case ret =>
          // user's code returns a value
          // return it to the caller directly
          returnResult(senderID, control.commandID, ret)
      }
    } catch {
      case e: Throwable =>
        // if error occurs, return it to the sender.
        returnResult(senderID, control.commandID, e)
    }
  }

  @inline
  private def returnResult(sender: ActorVirtualIdentity, id: Long, ret: Any): Unit = {
    controlOutputPort.sendTo(sender, ReturnPayload(id, ret))
  }

}
