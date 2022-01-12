package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.{
  QueryStatistics,
  QueryWorkerResult
}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.architecture.worker.{WorkerAsyncRPCHandlerInitializer, WorkerResult}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.{Constants, ITupleSinkOperatorExecutor}

object QueryStatisticsHandler {
  final case class QueryStatistics() extends ControlCommand[WorkerStatistics]
  final case class QueryWorkerResult() extends ControlCommand[Option[WorkerResult]]
}

trait QueryStatisticsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: QueryStatistics, sender) =>
    // report internal queue length if the gap > 30s
    val now = System.currentTimeMillis()
    if (now - lastReportTime > Constants.loggingQueueSizeInterval) {
      logger.info(
        s"Data Queue Length = ${dataProcessor.getDataQueueLength}, Control Queue Length = ${dataProcessor.getControlQueueLength}"
      )
      lastReportTime = now
    }

    // collect input and output row count
    val (in, out) = dataProcessor.collectStatistics()

    // sink operator doesn't output to downstream so internal count is 0
    // but for user-friendliness we show its input count as output count
    val displayOut = operator match {
      case sink: ITupleSinkOperatorExecutor =>
        in
      case _ =>
        out
    }

    val state = stateManager.getCurrentState

    WorkerStatistics(state, in, displayOut)
  }

  registerHandler((msg: QueryWorkerResult, sender) => {
    operator match {
      case sink: ITupleSinkOperatorExecutor =>
        Option(WorkerResult(sink.getOutputMode(), sink.getResultTuples()))
      case _ =>
        Option.empty
    }
  })

}
