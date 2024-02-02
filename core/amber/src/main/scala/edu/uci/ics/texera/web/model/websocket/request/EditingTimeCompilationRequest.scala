package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.workflow.{BreakpointInfo, LogicalLink}

case class EditingTimeCompilationRequest(
    operators: List[LogicalOp],
    links: List[LogicalLink],
    breakpoints: List[BreakpointInfo],
    opsToViewResult: List[String],
    opsToReuseResult: List[String]
) extends TexeraWebSocketRequest {

  def toLogicalPlanPojo: LogicalPlanPojo = {
    LogicalPlanPojo(operators, links, breakpoints, opsToViewResult, opsToReuseResult)
  }
}
