package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.texera.web.model.request.ResultPaginationRequest
import edu.uci.ics.texera.web.resource.WorkflowResultService.WebOutputMode

object PaginatedResultEvent {
  def apply(req: ResultPaginationRequest, table: List[ObjectNode]): PaginatedResultEvent = {
    PaginatedResultEvent(req.requestID, req.operatorID, req.pageIndex, table)
  }
}

case class PaginatedResultEvent(
    requestID: String,
    operatorID: String,
    pageIndex: Int,
    table: List[ObjectNode]
) extends TexeraWebSocketEvent

case class OperatorAvailableResult(
    cacheValid: Boolean,
    outputMode: WebOutputMode
)

case class WorkflowAvailableResultEvent(
    availableOperators: Map[String, OperatorAvailableResult]
) extends TexeraWebSocketEvent
