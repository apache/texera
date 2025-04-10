package edu.uci.ics.texera.web.model.websocket.response.python

import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatedValue
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

case class PythonExpressionEvaluateResponse(
    expression: String,
    values: Seq[EvaluatedValue]
) extends TexeraWebSocketEvent
