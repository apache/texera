package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  ExecutionStateUpdate,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object PauseHandler {

  final case class PauseWorkflow() extends ControlCommand[Unit]
}

/** pause the entire workflow
  *
  * possible sender: client, controller
  */
trait PauseHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[PauseWorkflow, Unit] { (msg, sender) =>
    {
      cp.controllerTimerService.disableStatusUpdate() // to be enabled in resume
      Future
        .collect(
          cp.workflowExecution.getRunningRegionExecutions
            .flatMap(_.getAllOperatorExecutions)
            .map {
              case (physicalOpId, opExecution) =>
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
                            .map { metrics =>
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
        .unit
    }
  }
}
