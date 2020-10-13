package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.principal.PrincipalStatistics

case class WorkflowStatusUpdateEvent(operatorStatistics: Map[String, PrincipalStatistics])
    extends TexeraWsEvent
