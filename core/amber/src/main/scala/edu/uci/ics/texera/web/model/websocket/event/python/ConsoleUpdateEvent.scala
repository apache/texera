package edu.uci.ics.texera.web.model.websocket.event.python

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.workflowruntimestate.ConsoleMessage

object ConsoleUpdateEvent {}

case class ConsoleUpdateEvent(
    operatorId: String,
    messages: Seq[ConsoleMessage]
) extends TexeraWebSocketEvent
