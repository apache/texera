package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.service.WorkflowResultService.WebResultUpdate

case class WebResultUpdateEvent(updates: Map[String, WebResultUpdate]) extends TexeraWebSocketEvent
