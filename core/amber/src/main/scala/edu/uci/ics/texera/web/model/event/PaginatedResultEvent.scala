package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.databind.node.ObjectNode

case class PaginatedOperatorResult(operatorID: String, table: List[ObjectNode], totalRowCount: Int)
case class PaginatedResultEvent(paginatedResults: List[PaginatedOperatorResult])
    extends TexeraWebSocketEvent
