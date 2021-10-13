package edu.uci.ics.texera.web.model.websocket.response

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

case class ResultExportResponse(status: String, message: String) extends TexeraWebSocketEvent
