package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.texera.workflow.LogicalLink

case class EditingTimeCompilationRequest(
    operators: List[LogicalOp],
    links: List[LogicalLink],
    opsToViewResult: List[String],
    opsToReuseResult: List[String]
) extends TexeraWebSocketRequest {

  def toLogicalPlanPojo: LogicalPlanPojo = {
    LogicalPlanPojo(operators, links, opsToViewResult, opsToReuseResult)
  }
}
