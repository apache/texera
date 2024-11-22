package edu.uci.ics.texera.web.model.websocket.event

case class ExecutionDurationUpdateEvent(duration: Long, isRunning: Boolean)
    extends TexeraWebSocketEvent
