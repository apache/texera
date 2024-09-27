package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerMetrics
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.CheckpointSupport
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object QueryStatisticsHandler {
  final case class QueryStatistics() extends ControlCommand[WorkerMetrics]
}

trait QueryStatisticsHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: QueryStatistics, sender) =>
    val internalState = if(dp.executor.isInstanceOf[CheckpointSupport]){
      dp.executor.asInstanceOf[CheckpointSupport].getState
    }else{
      "State not available"
    }
    WorkerMetrics(dp.stateManager.getCurrentState, dp.collectStatistics(), internalState)
  }

}
