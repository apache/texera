package edu.uci.ics.texera.web.model.event

case class WorkflowExecutionErrorEvent(message: String) extends TexeraWebSocketEvent
