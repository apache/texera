package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{BreakpointInfo, OperatorLink}

import scala.collection.mutable

case class WorkflowExecuteRequest(
    executionName: String,
    engineVersion: String,
    logicalPlan: LogicalPlanPojo
) extends TexeraWebSocketRequest

case class LogicalPlanPojo(
    operators: List[OperatorDescriptor],
    links: List[OperatorLink],
    breakpoints: List[BreakpointInfo],
    var cachedOperatorIds: List[String]
)
