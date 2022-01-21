package edu.uci.ics.texera.web.model.websocket.event

case class WorkflowExecutionErrorEvent(message: String) extends TexeraWebSocketEvent
