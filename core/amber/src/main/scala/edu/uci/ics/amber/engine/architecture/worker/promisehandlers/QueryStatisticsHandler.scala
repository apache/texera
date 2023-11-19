package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.ISinkOperatorExecutor

object QueryStatisticsHandler {
  final case class QueryStatistics() extends ControlCommand[WorkerStatistics]
}

trait QueryStatisticsHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: QueryStatistics, sender) =>
    // collect input and output row count
    val (in, out) = dp.collectStatistics()

    // sink operator doesn't output to downstream so internal count is 0
    // but for user-friendliness we show its input count as output count
    val displayOut = dp.operator match {
      case sink: ISinkOperatorExecutor =>
        in
      case _ =>
        out
    }

    val state = dp.stateManager.getCurrentState

    WorkerStatistics(state, in, displayOut)
  }

}
