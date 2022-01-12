package edu.uci.ics.texera.web.model.websocket.event

import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.texera.web.model.websocket.request.ResultPaginationRequest

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
