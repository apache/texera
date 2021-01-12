package edu.uci.ics.amber.engine.common.promise

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.promise.RPCServer.{AsyncRPCCommand, NormalRPCCommand}

object RPCServer {

  final val RootCallWithoutReturn = -1

  sealed trait RPCCommand[T]
  trait NormalRPCCommand[T] extends RPCCommand[T]
  trait AsyncRPCCommand[T] extends RPCCommand[T]
}

class RPCServer(controlOutputPort: ControlOutputPort) {

  // all handlers
  protected var normalHandlers: PartialFunction[NormalRPCCommand[_], Any] = PartialFunction.empty

  protected var asyncHandlers: PartialFunction[AsyncRPCCommand[_], Future[_]] =
    PartialFunction.empty

  def registerHandler[T](newHandler: PartialFunction[NormalRPCCommand[T], T]): Unit = {
    normalHandlers =
      newHandler.asInstanceOf[PartialFunction[NormalRPCCommand[_], Any]] orElse normalHandlers

  }

  def registerHandlerAsync[T](newHandler: PartialFunction[AsyncRPCCommand[T], Future[T]]): Unit = {
    asyncHandlers =
      newHandler.asInstanceOf[PartialFunction[AsyncRPCCommand[_], Future[_]]] orElse asyncHandlers
  }

  def execute(control: ControlInvocation, senderID: ActorVirtualIdentity): Unit = {
    try {
      control.call match {
        case command: AsyncRPCCommand[_] =>
          val f = asyncHandlers(command)
          f.onSuccess { ret =>
            returning(senderID, control.id, ret)
          }
          f.onFailure { err =>
            returning(senderID, control.id, err)
          }
        case command: NormalRPCCommand[_] =>
          val ret = normalHandlers(command)
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
