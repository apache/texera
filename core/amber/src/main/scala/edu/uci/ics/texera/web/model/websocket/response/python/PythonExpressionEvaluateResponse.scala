package edu.uci.ics.texera.web.model.websocket.response.python

import edu.uci.ics.amber.engine.architecture.worker.controlreturns.EvaluatedValue
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

case class PythonExpressionEvaluateResponse(
    expression: String,
    values: Seq[EvaluatedValue]
) extends TexeraWebSocketEvent
