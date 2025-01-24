package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, BroadcastMessageRequest, ContinueProcessingRequest, StopProcessingRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn

trait BroadcastMessageHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def broadcastMessage(request: BroadcastMessageRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    request.broadcastTo.foreach {
      physicalOp =>
        this.cp.workflowExecution.getAllRegionExecutions.find(x => x.hasOperatorExecution(physicalOp)).foreach {
          x =>
            x.getOperatorExecution(physicalOp).getWorkerIds.foreach {
              worker =>
                request.command match {
                  case cpr @ ContinueProcessingRequest(step) =>
                    workerInterface.continueProcessing(cpr, mkContext(worker))
                  case spr @ StopProcessingRequest() =>
                    workerInterface.stopProcessing(spr, mkContext(worker))
                  case other =>
                    // not supported yet.
                }
            }
        }
    }
    EmptyReturn()
  }
}