package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.error.WorkflowRuntimeError

object FatalErrorHandler {
  final case class FatalError(e: WorkflowRuntimeError) extends ControlCommand[CommandCompleted]
}

/** Indicate a fatal error has occurred in the workflow
  *
  * possible sender: controller, worker
  */
trait FatalErrorHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: FatalError, sender) =>
    {
      // log the error to console
      logger.logError(msg.e)
      // shutdown the workflow
      execute(KillWorkflow(), CONTROLLER)
    }
  }
}
