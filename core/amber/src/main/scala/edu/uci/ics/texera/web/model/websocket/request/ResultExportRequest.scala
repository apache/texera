package edu.uci.ics.texera.web.model.websocket.request

case class ResultExportRequest(
    exportType: String,
    workflowId: Int,
    workflowName: String,
    operatorId: String,
    operatorName: String,
    datasetIds: Array[Int],
    rowIndex: Int,
    columnIndex: Int,
    filename: String
) extends TexeraWebSocketRequest
