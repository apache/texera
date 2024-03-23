package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object FatalErrorHandler {
  final case class FatalError(e: Throwable, fromActor: Option[ActorVirtualIdentity] = None)
      extends ControlCommand[Unit]
}

/** Indicate a fatal error has occurred in the workflow
  *
  * possible sender: controller, worker
  */
trait FatalErrorHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[FatalError, Unit] { (msg, sender) =>
    {
      // log the error to console
      logger.error("FatalError received", msg)

      //report to client
      sendToClient(msg)
    }
  }
}
