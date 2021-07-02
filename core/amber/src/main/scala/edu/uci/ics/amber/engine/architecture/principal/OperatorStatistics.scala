package edu.uci.ics.amber.engine.architecture.principal

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode

case class OperatorStatistics(
    operatorState: OperatorState,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long
)

case class OperatorResult(
    outputMode: IncrementalOutputMode,
    result: List[ITuple]
)
