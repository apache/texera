package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

object WebWorkflowStatsUpdateEvent {
  def apply(update: WorkflowStatusUpdate): WebWorkflowStatsUpdateEvent = {
    WebWorkflowStatsUpdateEvent(update.operatorStatistics)
  }
}

case class WebWorkflowStatsUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent
