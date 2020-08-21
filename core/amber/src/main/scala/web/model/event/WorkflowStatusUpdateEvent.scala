package web.model.event

import Engine.Architecture.Principal.PrincipalStatistics

case class WorkflowStatusUpdateEvent(operatorStatistics: Map[String, PrincipalStatistics])
    extends TexeraWsEvent
