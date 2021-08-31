package edu.uci.ics.texera.web.model.event

case class CacheStatusUpdateEvent(cacheStatusMap: Map[String, CacheStatus]) extends TexeraWebSocketEvent
