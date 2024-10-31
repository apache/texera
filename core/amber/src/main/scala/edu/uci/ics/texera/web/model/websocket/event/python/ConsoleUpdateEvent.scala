package edu.uci.ics.texera.web.model.websocket.event.python

import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessage
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

object ConsoleUpdateEvent {}

case class ConsoleUpdateEvent(
    operatorId: String,
    messages: Seq[ConsoleMessage]
) extends TexeraWebSocketEvent
