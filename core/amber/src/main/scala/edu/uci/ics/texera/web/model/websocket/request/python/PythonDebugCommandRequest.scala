package edu.uci.ics.texera.web.model.websocket.request.python

import edu.uci.ics.texera.web.model.websocket.request.TexeraWebSocketRequest

case class PythonDebugCommandRequest(operatorId: String, workerId: String, cmd: String)
    extends TexeraWebSocketRequest
