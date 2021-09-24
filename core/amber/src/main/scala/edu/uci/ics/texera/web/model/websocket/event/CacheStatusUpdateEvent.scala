package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.model.common.CacheStatus

case class CacheStatusUpdateEvent(cacheStatusMap: Map[String, CacheStatus])
    extends TexeraWebSocketEvent
