package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{EmptyRequest, AsyncRPCContext}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerMetricsResponse
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerMetrics
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

trait QueryStatisticsHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def queryStatistics(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerMetricsResponse] = {
    WorkerMetricsResponse(WorkerMetrics(dp.stateManager.getCurrentState, dp.collectStatistics()))
  }

}
