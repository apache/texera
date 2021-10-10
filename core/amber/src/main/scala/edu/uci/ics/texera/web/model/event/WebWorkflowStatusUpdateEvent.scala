package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

object WebWorkflowStatusUpdateEvent {
  def apply(update: WorkflowStatusUpdate): WebWorkflowStatusUpdateEvent = {
    WebWorkflowStatusUpdateEvent(update.operatorStatistics)
  }
}

case class WebWorkflowStatusUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent
    with StateSynchronization
