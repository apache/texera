package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.EvaluateExpressionHandler.EvaluateExpression
import edu.uci.ics.amber.engine.architecture.worker.controlreturns.EvaluatedValue
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity

object EvaluatePythonExpressionHandler {
  final case class EvaluatePythonExpression(expression: String, operatorId: String)
      extends ControlCommand[List[EvaluatedValue]]
}

trait EvaluatePythonExpressionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: EvaluatePythonExpression, sender) =>
    {
      val logicalOpId = new OperatorIdentity(msg.operatorId)
      val physicalOps = cp.workflow.physicalPlan.getPhysicalOpsOfLogicalOp(logicalOpId)
      if (physicalOps.size != 1) {
        val msg =
          s"logical operator $logicalOpId has ${physicalOps.size} physical operators, expecting a single one"
        throw new RuntimeException(msg)
      }

      val physicalOp = physicalOps.head
      val opExecution = cp.executionState.getOperatorExecution(physicalOp.id)

      Future
        .collect(
          opExecution.getBuiltWorkerIds
            .map(worker => send(EvaluateExpression(msg.expression), worker))
            .toList
        )
        .map(evaluatedValues => {
          evaluatedValues.toList
        })
    }
  }
}
