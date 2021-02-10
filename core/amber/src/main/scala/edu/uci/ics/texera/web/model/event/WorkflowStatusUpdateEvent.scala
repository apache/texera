package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.principal.OperatorStatistics

case class WorkflowStatusUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent
