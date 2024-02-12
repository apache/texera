package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatsUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ResumeHandler {
  final case class ResumeWorkflow() extends ControlCommand[Unit]
}

/** resume the entire workflow
  *
  * possible sender: controller, client
  */
trait ResumeHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[ResumeWorkflow, Unit] { (msg, sender) =>
    {

      // send all workers resume
      // resume message has no effect on non-paused workers
      Future
        .collect(cp.executionState.getAllBuiltWorkers.map { workerId =>
          send(ResumeWorker(), workerId).map { ret =>
            cp.executionState.getOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(workerId)).getWorkerExecution(workerId).state = ret
          }
        }.toSeq)
        .map { _ =>
          // update frontend status
          sendToClient(WorkflowStatsUpdate(cp.executionState.getStats))
          cp.controllerTimerService
            .enableStatusUpdate() //re-enabled it since it is disabled in pause
          cp.controllerTimerService.enableMonitoring()
          cp.controllerTimerService.enableSkewHandling()
        }
    }
  }
}
