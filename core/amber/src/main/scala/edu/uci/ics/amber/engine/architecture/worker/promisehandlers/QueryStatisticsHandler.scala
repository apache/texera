package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerMetricsResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerMetrics

trait QueryStatisticsHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def queryStatistics(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerMetricsResponse] = {
    WorkerMetricsResponse(WorkerMetrics(dp.stateManager.getCurrentState, dp.collectStatistics()))
  }

}
