package edu.uci.ics.amber.engine.architecture.principal

case class OperatorStatistics(
    operatorState: OperatorState,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long
)
