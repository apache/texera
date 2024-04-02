package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerInfo
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object QueryStatisticsHandler {
  final case class QueryStatistics() extends ControlCommand[WorkerInfo]
}

trait QueryStatisticsHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: QueryStatistics, sender) =>
    WorkerInfo(dp.stateManager.getCurrentState, dp.collectStatistics())
  }

}
