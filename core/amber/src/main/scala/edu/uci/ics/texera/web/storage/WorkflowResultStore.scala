package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity

case class OperatorResultMetadata(tupleCount: Int = 0, changeDetector: String = "")

case class WorkflowResultStore(
    resultInfo: Map[OperatorIdentity, OperatorResultMetadata] = Map.empty
)
