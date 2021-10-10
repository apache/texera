package edu.uci.ics.texera.web.model.event

case class RegisterWIdResponse(message: String) extends TexeraWebSocketEvent

case class HeartBeatResponse() extends TexeraWebSocketEvent
