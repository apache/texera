package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{
  WorkerAsyncRPCHandlerInitializer,
  WorkerStatistics
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.ITupleSinkOperatorExecutor
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object QueryStatisticsHandler {
  final case class QueryStatistics() extends ControlCommand[WorkerStatistics]
}

trait QueryStatisticsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: QueryStatistics, sender) =>
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
    val result = operator match {
      case sink: ITupleSinkOperatorExecutor =>
        Option(sink.getResultTuples())
      case _ =>
        Option.empty
    }

    WorkerStatistics(state, in, displayOut, result)
  }

}
