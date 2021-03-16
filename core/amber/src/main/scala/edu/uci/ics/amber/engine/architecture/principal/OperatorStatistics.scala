package edu.uci.ics.amber.engine.architecture.principal

import edu.uci.ics.amber.engine.common.tuple.ITuple

case class OperatorStatistics(
    operatorState: OperatorState,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long,
    aggregatedOutputResults: Option[List[ITuple]] // in case of a sink operator
)
