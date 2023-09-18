package edu.uci.ics.texera.web.storage

case class OperatorResultMetadata(tupleCount: Int = 0, changeDetector: String = "")

case class WorkflowResultStore(
    resultInfo: Map[String, OperatorResultMetadata] = Map.empty
)
