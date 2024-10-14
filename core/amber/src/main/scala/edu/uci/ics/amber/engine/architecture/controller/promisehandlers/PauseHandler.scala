package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, PauseWorkflowRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable


/** pause the entire workflow
  *
  * possible sender: client, controller
  */
trait PauseHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def sendPauseWorkflow(request: PauseWorkflowRequest, ctx: AsyncRPCContext): Future[Empty] = {
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
                      send(PauseWorker(), worker).flatMap { state =>
                        workerExecution.setState(state)
                        send(QueryStatistics(), worker)
                          // get the stats and current input tuple from the worker
                          .map {
                            case (metrics, tuple) =>
                              workerExecution.setStats(metrics.workerStatistics)
                              buffer.append((tuple, worker))
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
      .unit
  }

}
