package edu.uci.ics.texera.web.model.request

case class ResultExportRequest(exportType: String, workflowName: String, operatorId: String)
    extends TexeraWebSocketRequest
