package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ExecutionStartedHandler.ExecutionStarted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running

object ExecutionStartedHandler {
  final case class ExecutionStarted() extends ControlCommand[CommandCompleted]
}

trait ExecutionStartedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ExecutionStarted, sender) =>
    workflow.getOperator(sender).getWorker(sender).state = Running
    CommandCompleted()
  }
}
