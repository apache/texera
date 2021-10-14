package edu.uci.ics.texera.web.model.websocket.request

case class ResultPaginationRequest(
    requestID: String,
    operatorID: String,
    pageIndex: Int,
    pageSize: Int
) extends TexeraWebSocketRequest
