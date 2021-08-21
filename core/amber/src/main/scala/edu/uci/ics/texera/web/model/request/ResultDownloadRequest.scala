package edu.uci.ics.texera.web.model.request

case class ResultDownloadRequest(downloadType: String, workflowName: String, operatorId: String)
    extends TexeraWebSocketRequest
