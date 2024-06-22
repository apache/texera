package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.service.ExecutionResultService.WebResultUpdate

case class WebResultUpdateEvent(
    updates: Map[String, WebResultUpdate],
    tableStats: Map[String, Map[String, Map[String, Any]]],
    sinkStorageMode: String
) extends TexeraWebSocketEvent
