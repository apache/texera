package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.RUNNING

object StartWorkflowHandler {
  final case class StartWorkflow() extends ControlCommand[WorkflowAggregatedState]
}

/** start the workflow by starting the source workers
  * note that this SHOULD only be called once per workflow
  *
  * possible sender: client
  */
trait StartWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StartWorkflow, sender) =>
    {
      if (cp.workflowExecution.getState.isUninitialized) {
        cp.workflowExecutionCoordinator
          .executeNextRegions(cp.actorService)
          .map(_ => {
            cp.controllerTimerService.enableStatusUpdate()
            RUNNING
          })
      } else {
        Future(cp.workflowExecution.getState)
      }
    }
  }
}
