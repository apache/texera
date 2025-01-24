package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerMetricsResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerMetrics
import edu.uci.ics.amber.engine.common.CheckpointSupport

trait QueryStatisticsHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def queryStatistics(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerMetricsResponse] = {
    val internalState = dp.executor match{
      case c: CheckpointSupport =>
        c.getState
      case other =>
        ""
    }
    WorkerMetricsResponse(WorkerMetrics(dp.stateManager.getCurrentState, dp.collectStatistics(), internalState))
  }

}
