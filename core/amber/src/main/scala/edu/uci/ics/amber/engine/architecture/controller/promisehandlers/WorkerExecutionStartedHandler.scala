package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.worker.WorkerState

object WorkerExecutionStartedHandler {
  final case class WorkerStateUpdated(state: WorkerState) extends ControlCommand[CommandCompleted]
}

/** indicate the state change of a worker
  *
  * possible sender: worker
  */
trait WorkerExecutionStartedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerStateUpdated, sender) =>
    {
      // set the state
      workflow.getOperator(sender).getWorker(sender).state = msg.state
      updateFrontendWorkflowStatus()
      CommandCompleted()
    }
  }
}
