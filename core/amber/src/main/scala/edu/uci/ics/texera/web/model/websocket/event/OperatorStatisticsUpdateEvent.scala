package edu.uci.ics.texera.web.model.websocket.event

case class OperatorStatistics(
    operatorState: String,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long
)

case class OperatorStatisticsUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent
