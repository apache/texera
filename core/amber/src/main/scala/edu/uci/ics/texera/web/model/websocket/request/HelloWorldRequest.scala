package edu.uci.ics.texera.web.model.websocket.request

case class HelloWorldRequest(message: String) extends TexeraWebSocketRequest

case class HeartBeatRequest() extends TexeraWebSocketRequest
