package edu.uci.ics.texera.web.model.common.python

import edu.uci.ics.amber.engine.architecture.worker.controlreturns.EvaluatedValue
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

case class PythonExpressionEvaluateResponse(
    expression: String,
    values: List[EvaluatedValue]
) extends TexeraWebSocketEvent
