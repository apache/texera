package edu.uci.ics.texera.web.model.websocket.event

case class CacheStatusUpdateEvent(cacheStatusMap: Map[String, String]) extends TexeraWebSocketEvent
