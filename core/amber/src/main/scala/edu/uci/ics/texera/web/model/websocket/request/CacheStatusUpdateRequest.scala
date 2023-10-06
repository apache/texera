package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{BreakpointInfo, OperatorLink}

case class CacheStatusUpdateRequest(
    operators: List[OperatorDescriptor],
    links: List[OperatorLink],
    breakpoints: List[BreakpointInfo],
    opsToViewResult: List[String],
    opsToReuseResult: List[String]
) extends TexeraWebSocketRequest {

  def toLogicalPlanPojo() = {
    LogicalPlanPojo(operators, links, breakpoints, opsToViewResult, opsToReuseResult)
  }
}
