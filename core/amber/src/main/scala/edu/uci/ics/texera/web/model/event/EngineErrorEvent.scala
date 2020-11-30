package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.common.error.EngineError

case class EngineErrorEvent (error: EngineError) extends TexeraWebSocketEvent
