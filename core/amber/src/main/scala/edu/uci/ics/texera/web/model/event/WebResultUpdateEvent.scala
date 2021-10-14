package edu.uci.ics.texera.web.model.event

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.service.WorkflowResultService.WebResultUpdate

case class WebResultUpdateEvent(updates: Map[String, WebResultUpdate]) extends TexeraWebSocketEvent
