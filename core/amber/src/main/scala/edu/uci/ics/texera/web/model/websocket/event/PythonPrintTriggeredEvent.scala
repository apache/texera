package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent

object PythonPrintTriggeredEvent {
  def apply(event: ControllerEvent.PythonPrintTriggered): PythonPrintTriggeredEvent = {
    PythonPrintTriggeredEvent(event.message, event.operatorID)
  }
}

case class PythonPrintTriggeredEvent(
    message: String,
    operatorID: String
) extends TexeraWebSocketEvent
