package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  ReportCurrentProcessingTuple,
  WorkflowPaused,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

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

  registerHandler { (msg: PauseWorkflow, sender) =>
    {
      cp.controllerTimerService.disableStatusUpdate() // to be enabled in resume
      cp.controllerTimerService.disableMonitoring()
      cp.controllerTimerService.disableSkewHandling()
      Future
        .collect(cp.executionState.getAllOperatorExecutions.map {
          case (layerId, opExecution) =>
            // create a buffer for the current input tuple
            // since we need to show them on the frontend
            val buffer = mutable.ArrayBuffer[(ITuple, ActorVirtualIdentity)]()
            Future
              .collect(
                opExecution.getBuiltWorkerIds
                  // send pause to all workers
                  // pause message has no effect on completed or paused workers
                  .map { worker =>
                    val info = opExecution.getWorkerInfo(worker)
                    // send a pause message
                    send(PauseWorker(), worker).flatMap { ret =>
                      info.state = ret
                      send(QueryStatistics(), worker)
                        .join(send(QueryCurrentInputTuple(), worker))
                        // get the stats and current input tuple from the worker
                        .map {
                          case (stats, tuple) =>
                            info.stats = stats
                            buffer.append((tuple, worker))
                        }
                    }
                  }.toSeq
              )
              .map { ret =>
                // for each paused operator, send the input tuple
                sendToClient(ReportCurrentProcessingTuple(layerId.operator, buffer.toArray))
              }
        }.toSeq)
        .map { ret =>
          // update frontend workflow status
          sendToClient(WorkflowStatusUpdate(cp.executionState.getWorkflowStatus))
          sendToClient(WorkflowPaused())
          logger.info(s"workflow paused")
        }
        .unit
    }
  }

}
