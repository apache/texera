package edu.uci.ics.texera.web.model.request.python

import edu.uci.ics.texera.web.model.request.TexeraWebSocketRequest

case class PythonExpressionEvaluateRequest(expression: String, operatorId: String) extends TexeraWebSocketRequest
