package edu.uci.ics.texera.web.model.websocket.request

case class ResultExportRequest(exportType: String, workflowName: String, operatorId: String)
    extends TexeraWebSocketRequest
