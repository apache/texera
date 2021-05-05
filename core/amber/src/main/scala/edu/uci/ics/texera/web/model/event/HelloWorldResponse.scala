package edu.uci.ics.texera.web.model.event

case class HelloWorldResponse(message: String) extends TexeraWebSocketEvent

case class HeartBeatResponse() extends TexeraWebSocketEvent
