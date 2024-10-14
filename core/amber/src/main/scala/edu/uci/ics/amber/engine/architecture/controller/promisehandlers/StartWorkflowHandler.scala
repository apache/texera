package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, StartWorkflowRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.RUNNING


/** start the workflow by starting the source workers
  * note that this SHOULD only be called once per workflow
  *
  * possible sender: client
  */
trait StartWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def sendStartWorkflow(request: StartWorkflowRequest, ctx: AsyncRPCContext): Future[StartWorkflowResponse] = {
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
