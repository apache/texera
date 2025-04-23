package edu.uci.ics.texera.web.model.http.request.result

case class ResultExportRequest(
    exportType: String, // e.g. "csv", "google_sheet", "arrow", "data"
    workflowId: Int,
    workflowName: String,
    operatorIds: List[String], // changed from single operatorId: String -> List of strings
    datasetIds: List[Int],
    rowIndex: Int, // used by "data" export
    columnIndex: Int, // used by "data" export
    filename: String, // optional filename override
    destination: String // "dataset" or "local"
)
