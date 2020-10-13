package web.model.event

import engine.architecture.principal.PrincipalStatistics

case class WorkflowStatusUpdateEvent(operatorStatistics: Map[String, PrincipalStatistics])
    extends TexeraWsEvent
