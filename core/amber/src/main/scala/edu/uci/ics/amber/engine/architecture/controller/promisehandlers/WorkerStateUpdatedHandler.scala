package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  WorkerStateUpdatedRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils

/** indicate the state change of a worker
  *
  * possible sender: worker
  */
trait WorkerStateUpdatedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def workerStateUpdated(
      msg: WorkerStateUpdatedRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    val physicalOpId = VirtualIdentityUtils.getPhysicalOpId(ctx.sender)
    // set the state
    cp.workflowExecution.getRunningRegionExecutions
      .find(_.hasOperatorExecution(physicalOpId))
      .map(_.getOperatorExecution(physicalOpId))
      .foreach(operatorExecution =>
        operatorExecution.getWorkerExecution(ctx.sender).setState(msg.state)
      )
    sendToClient(
      ExecutionStatsUpdate(
        cp.workflowExecution.getAllRegionExecutionsStats
      )
    )
    EmptyReturn()
  }
}
