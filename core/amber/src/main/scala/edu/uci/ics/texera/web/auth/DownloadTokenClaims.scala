package edu.uci.ics.texera.web.auth

case class DownloadTokenClaims(
    exportType: String,
    workflowId: Int,
    workflowName: String,
    rowIndex: Int,
    columnIndex: Int,
    filename: String,
    computingUnitId: Int,
    destination: String
)
