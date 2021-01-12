package edu.uci.ics.amber.engine.common.control

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.control.ControlMessageSource.{
  ControlInvocation,
  ReturnPayload
}
import edu.uci.ics.amber.engine.common.control.ControlMessageReceiver.ControlCommand

object ControlMessageReceiver {

  trait ControlCommand[T]
}

class ControlMessageReceiver(controlOutputPort: ControlOutputPort) {

  // all handlers
  protected var handlers: PartialFunction[ControlCommand[_], Any] = PartialFunction.empty

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
