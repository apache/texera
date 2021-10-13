package edu.uci.ics.texera.web.model.websocket.request.python

import edu.uci.ics.texera.web.model.websocket.request.TexeraWebSocketRequest

case class PythonExpressionEvaluateRequest(expression: String, operatorId: String)
    extends TexeraWebSocketRequest
