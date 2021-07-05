package edu.uci.ics.texera.web.model.request

case class ResultPaginationRequest(
    requestID: String,
    operatorID: String,
    pageIndex: Int,
    pageSize: Int
) extends TexeraWebSocketRequest
