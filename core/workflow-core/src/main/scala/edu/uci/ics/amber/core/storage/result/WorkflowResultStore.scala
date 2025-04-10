package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity

case class OperatorResultMetadata(tupleCount: Int = 0, changeDetector: String = "")

case class WorkflowResultStore(
    resultInfo: Map[OperatorIdentity, OperatorResultMetadata] = Map.empty
)
