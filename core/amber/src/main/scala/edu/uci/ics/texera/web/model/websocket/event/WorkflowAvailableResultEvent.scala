package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.model.websocket.event.WorkflowAvailableResultEvent.OperatorAvailableResult
import edu.uci.ics.texera.web.service.JobResultService.WebOutputMode

object WorkflowAvailableResultEvent {
  case class OperatorAvailableResult(
      cacheValid: Boolean,
      outputMode: WebOutputMode
  )
}
case class WorkflowAvailableResultEvent(
    availableOperators: Map[String, OperatorAvailableResult]
) extends TexeraWebSocketEvent
