package edu.uci.ics.texera.web.model.websocket.request

case class SkipTupleRequest(workerIds: Array[String]) extends TexeraWebSocketRequest
