package edu.uci.ics.amber.storage.result

import edu.uci.ics.amber.virtualidentity.OperatorIdentity

case class OperatorResultMetadata(tupleCount: Int = 0, changeDetector: String = "")

case class WorkflowResultStore(
    resultInfo: Map[OperatorIdentity, OperatorResultMetadata] = Map.empty
)
