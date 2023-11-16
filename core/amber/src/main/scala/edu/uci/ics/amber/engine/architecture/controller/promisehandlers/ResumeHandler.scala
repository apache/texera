package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
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

  registerHandler { (msg: ResumeWorkflow, sender) =>
    {

      // send all workers resume
      // resume message has no effect on non-paused workers
      Future
        .collect(cp.executionState.getAllBuiltWorkers.map { worker =>
          send(ResumeWorker(), worker).map { ret =>
            cp.executionState.getOperatorExecution(worker).getWorkerInfo(worker).state = ret
          }
        }.toSeq)
        .map { _ =>
          // update frontend status
          sendToClient(WorkflowStatusUpdate(cp.executionState.getWorkflowStatus))
          cp.controllerTimerService
            .enableStatusUpdate() //re-enabled it since it is disabled in pause
          cp.controllerTimerService.enableMonitoring()
          cp.controllerTimerService.enableSkewHandling()
        }
    }
  }
}
