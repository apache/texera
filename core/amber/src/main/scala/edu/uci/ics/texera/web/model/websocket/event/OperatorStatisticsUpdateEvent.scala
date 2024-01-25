package edu.uci.ics.texera.web.model.websocket.event

case class OperatorStatistics(
    operatorState: String,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long,
    numWorkers: Long,
    aggregatedDataProcessingTime: Long,
    aggregatedControlProcessingTime: Long,
    aggregatedIdleTime: Long
)

case class OperatorStatisticsUpdateEvent(operatorStatistics: Map[String, OperatorStatistics])
    extends TexeraWebSocketEvent
