package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  ReportCurrentProcessingTuple,
  ExecutionStatsUpdate,
  ExecutionStateUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

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
                            .join(send(QueryCurrentInputTuple(), worker))
                            // get the stats and current input tuple from the worker
                            .map {
                              case (stats, tuple) =>
                                workerExecution.setStats(stats)
                                buffer.append((tuple, worker))
                            }
                        }
                      }.toSeq
                  )
                  .map { _ =>
                    // for each paused operator, send the input tuple
                    sendToClient(
                      ReportCurrentProcessingTuple(physicalOpId.logicalOpId.id, buffer.toArray)
                    )
                  }
            }
            .toSeq
        )
        .map { _ =>
          // update frontend workflow status
          sendToClient(
            ExecutionStatsUpdate(
              cp.workflowExecution.getRunningRegionExecutions.flatMap(_.getStats).toMap
            )
          )
          sendToClient(ExecutionStateUpdate(cp.workflowExecution.getState))
          logger.info(s"workflow paused")
        }
        .unit
    }
  }
}
