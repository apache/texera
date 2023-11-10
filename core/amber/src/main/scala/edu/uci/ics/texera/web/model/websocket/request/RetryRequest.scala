package edu.uci.ics.texera.web.model.websocket.request

case class RetryRequest(workers: Seq[String]) extends TexeraWebSocketRequest
