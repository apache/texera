package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStateUpdate,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{EmptyReturn, WorkerMetricsResponse}
import edu.uci.ics.amber.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

/** pause the entire workflow
  *
  * possible sender: client, controller
  */
trait PauseHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def pauseWorkflow(request: EmptyRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    cp.controllerTimerService.disableStatusUpdate() // to be enabled in resume
    Future
      .collect(
        cp.workflowExecution.getRunningRegionExecutions
          .flatMap(_.getAllOperatorExecutions)
          .map {
            case (physicalOpId, opExecution) =>
              // create a buffer for the current input tuple
              // since we need to show them on the frontend
              val buffer = mutable.ArrayBuffer[(Tuple, ActorVirtualIdentity)]()
              Future
                .collect(
                  opExecution.getWorkerIds
                    // send pause to all workers
                    // pause message has no effect on completed or paused workers
                    .map { worker =>
                      val workerExecution = opExecution.getWorkerExecution(worker)
                      // send a pause message
                      workerInterface.pauseWorker(EmptyRequest(), mkContext(worker)).flatMap {
                        resp =>
                          workerExecution.setState(resp.state)
                          workerInterface
                            .queryStatistics(EmptyRequest(), mkContext(worker))
                            // get the stats and current input tuple from the worker
                            .map {
                              case WorkerMetricsResponse(metrics) =>
                                workerExecution.setStats(metrics.workerStatistics)
                            }
                      }
                    }.toSeq
                )
          }
          .toSeq
      )
      .map { _ =>
        // update frontend workflow status
        sendToClient(
          ExecutionStatsUpdate(
            cp.workflowExecution.getAllRegionExecutionsStats
          )
        )
        sendToClient(ExecutionStateUpdate(cp.workflowExecution.getState))
        logger.info(s"workflow paused")
      }
    EmptyReturn()
  }

}
