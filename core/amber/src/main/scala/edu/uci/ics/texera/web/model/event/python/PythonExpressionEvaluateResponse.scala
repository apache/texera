package edu.uci.ics.texera.web.model.event.python

import edu.uci.ics.amber.engine.architecture.worker.controlreturns.EvaluatedValue
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.web.model.event.TexeraWebSocketEvent

case class PythonExpressionEvaluateResponse(
    expression: String,
    values: List[EvaluatedValue]
) extends TexeraWebSocketEvent
    with ControlCommand[Unit]
