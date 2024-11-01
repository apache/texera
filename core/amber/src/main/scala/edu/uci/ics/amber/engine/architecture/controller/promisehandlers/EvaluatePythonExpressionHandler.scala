package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EvaluatePythonExpressionRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity

trait EvaluatePythonExpressionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def evaluatePythonExpression(
      msg: EvaluatePythonExpressionRequest,
      ctx: AsyncRPCContext
  ): Future[EvaluatePythonExpressionResponse] = {
    val logicalOpId = new OperatorIdentity(msg.operatorId)
    val physicalOps = cp.workflowScheduler.physicalPlan.getPhysicalOpsOfLogicalOp(logicalOpId)
    if (physicalOps.size != 1) {
      val msg =
        s"logical operator $logicalOpId has ${physicalOps.size} physical operators, expecting a single one"
      throw new RuntimeException(msg)
    }

    val physicalOp = physicalOps.head
    val opExecution = cp.workflowExecution.getLatestOperatorExecution(physicalOp.id)

    Future
      .collect(
        opExecution.getWorkerIds
          .map(worker => workerInterface.evaluatePythonExpression(msg, mkContext(worker)))
          .toList
      )
      .map(evaluatedValues => {
        EvaluatePythonExpressionResponse(evaluatedValues)
      })
  }

}
