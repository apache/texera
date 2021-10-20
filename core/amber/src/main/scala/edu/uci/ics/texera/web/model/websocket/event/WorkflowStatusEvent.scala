package edu.uci.ics.texera.web.model.websocket.event

case class WorkflowStatusEvent(status: ExecutionStatusEnum) extends TexeraWebSocketEvent
