package edu.uci.ics.texera.web.model.websocket.event.python

import com.google.protobuf.timestamp.Timestamp
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent

object PythonConsoleUpdateEvent {
  def apply(event: ControllerEvent.PythonConsoleMessageTriggered): PythonConsoleUpdateEvent = {
    PythonConsoleUpdateEvent(
      event.operatorId,
      event.workerId,
      event.consoleMessage.timestamp,
      event.consoleMessage.msgType,
      event.consoleMessage.message
    )
  }
}

case class PythonConsoleUpdateEvent(
    operatorId: String,
    workerId: String,
    timestamp: Timestamp,
    msgType: String,
    message: String
) extends TexeraWebSocketEvent
