package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.EvaluateExpressionHandler.EvaluateExpression
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.web.model.event.python.PythonExpressionEvaluateResponse

object EvaluatePythonExpressionHandler {
  final case class EvaluatePythonExpression(expression: String) extends ControlCommand[Unit]
}

trait EvaluatePythonExpressionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: EvaluatePythonExpression, sender) =>
    {

      Future
        .collect(
          workflow.getPythonWorkers
            .map(worker => send(EvaluateExpression(msg.expression), worker))
            .toList
        )
        .onSuccess(evaluatedValues => {
          if (eventListener.pythonExpressionEvaluatedListener != null) {
            eventListener.pythonExpressionEvaluatedListener
              .apply(PythonExpressionEvaluateResponse(msg.expression, evaluatedValues.toList))
          }
        })
        .unit
    }
  }
}
