package edu.uci.ics.amber.engine.common.promise

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

object RPCServer {

  final val RootCallWithoutReturn = -1

  trait RPCCommand[T]
}

class RPCServer(controlOutputPort: ControlOutputPort) {

  // all handlers
  protected var handlers: PartialFunction[RPCCommand[_], Any] = PartialFunction.empty

  def registerHandler(newHandler: PartialFunction[RPCCommand[_], Any]): Unit = {
    handlers =
      newHandler orElse handlers

  }

  def execute(control: ControlInvocation, senderID: ActorVirtualIdentity): Unit = {
    try {
      handlers(control.call) match {
        case f: Future[_] =>
          f.onSuccess { ret =>
            returning(senderID, control.id, ret)
          }
          f.onFailure { err =>
            returning(senderID, control.id, err)
          }
        case ret =>
          returning(senderID, control.id, ret)
      }
    } catch {
      case e: Throwable =>
        // if error occurs, return it to the sender.
        returning(senderID, control.id, e)
    }
  }

  private def returning(sender: ActorVirtualIdentity, id: Long, ret: Any): Unit = {
    if (id != RPCServer.RootCallWithoutReturn) {
      controlOutputPort.sendTo(sender, ReturnPayload(id, ret))
    }
  }

}
