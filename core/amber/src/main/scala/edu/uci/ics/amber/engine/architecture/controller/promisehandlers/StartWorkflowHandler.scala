package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.RUNNING
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse

/** start the workflow by starting the source workers
  * note that this SHOULD only be called once per workflow
  *
  * possible sender: client
  */
trait StartWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def startWorkflow(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[StartWorkflowResponse] = {
    if (cp.workflowExecution.getState.isUninitialized) {
      cp.workflowExecutionCoordinator
        .executeNextRegions(cp.actorService)
        .map(_ => {
          cp.controllerTimerService.enableStatusUpdate()
          StartWorkflowResponse(RUNNING)
        })
    } else {
      StartWorkflowResponse(cp.workflowExecution.getState)
    }
  }

}
