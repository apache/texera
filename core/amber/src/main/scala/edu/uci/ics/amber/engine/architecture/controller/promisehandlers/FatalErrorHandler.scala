package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError

object FatalErrorHandler {
  final case class FatalError(e: WorkflowRuntimeError) extends ControlCommand[CommandCompleted]
}

trait FatalErrorHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: FatalError, sender) =>
    logger.logError(msg.e)
    execute(KillWorkflow(), ActorVirtualIdentity.Controller)
  }
}
