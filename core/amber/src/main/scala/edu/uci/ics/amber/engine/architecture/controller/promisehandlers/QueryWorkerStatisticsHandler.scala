package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.util.VirtualIdentityUtils

/** Get statistics from all the workers
  *
  * possible sender: controller(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def controllerInitiateQueryStatistics(
      msg: QueryStatisticsRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // send to specified workers (or all workers by default)
    val workers = if (msg.filterByWorkers.nonEmpty) {
      msg.filterByWorkers
    } else {
      cp.workflowExecution.getAllRegionExecutions
        .flatMap(_.getAllOperatorExecutions.map(_._2))
        .flatMap(_.getWorkerIds)
    }

    // send QueryStatistics message
    val requests = workers
      .map(workerId =>
        // must immediately update worker state and stats after reply
        workerInterface
          .queryStatistics(EmptyRequest(), workerId)
          .map(resp => {
            val workerExecution =
              cp.workflowExecution
                .getLatestOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(workerId))
                .getWorkerExecution(workerId)
            workerExecution.setState(resp.metrics.workerState)
            workerExecution.setStats(resp.metrics.workerStatistics)
          })
      )
      .toSeq

    // wait for all workers to reply before notifying frontend
    Future
      .collect(requests)
      .map(_ =>
        sendToClient(
          ExecutionStatsUpdate(
            cp.workflowExecution.getAllRegionExecutionsStats
          )
        )
      )
    EmptyReturn()
  }

}
