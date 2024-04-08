package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.ExecutionStatsUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object QueryWorkerStatisticsHandler {

  final case class ControllerInitiateQueryStatistics(
      filterByWorkers: Option[List[ActorVirtualIdentity]] = None
  ) extends ControlCommand[Unit]

}

/** Get statistics from all the workers
  *
  * possible sender: controller(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[ControllerInitiateQueryStatistics, Unit]((msg, sender) => {
    // send to specified workers (or all workers by default)
    val workers = msg.filterByWorkers.getOrElse(
      cp.workflowExecution.getAllRegionExecutions
        .flatMap(_.getAllOperatorExecutions.map(_._2))
        .flatMap(_.getWorkerIds)
    )

    // send QueryStatistics message
    val requests = workers
      .map(workerId =>
        // must immediately update worker state and stats after reply
        send(QueryStatistics(), workerId).map(metrics => {
          val workerExecution =
            cp.workflowExecution
              .getLatestOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(workerId))
              .getWorkerExecution(workerId)
          workerExecution.setState(metrics.workerState)
          workerExecution.setStats(metrics.workerStatistics)
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
  })
}
