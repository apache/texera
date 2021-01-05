package edu.uci.ics.texera.web.model.request

case class ResultPaginationRequest(pageIndex: Int, pageSize: Int) extends TexeraWebSocketRequest
