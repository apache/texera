package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.controlreturns.EvaluatedValue
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object EvaluateExpressionHandler {
  final case class EvaluateExpression(expression: String) extends ControlCommand[EvaluatedValue]
}
