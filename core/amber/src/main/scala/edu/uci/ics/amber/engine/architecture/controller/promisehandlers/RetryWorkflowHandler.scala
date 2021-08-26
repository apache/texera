package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ControllerState
}
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.RetryPythonHandler.RetryPython
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object RetryWorkflowHandler {
  final case class RetryWorkflow() extends ControlCommand[Unit]
}

/** retry the execution of the entire workflow
  *
  * possible sender: controller, client
  */
trait RetryWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: RetryWorkflow, sender) =>
    {
      // if it is a PythonWorker, prepare for retry
      // retry message has no effect on completed workers
      Future
        .collect(workflow.getPythonWorkers.map { worker => send(RetryPython(), worker) }.toSeq)
        .map { _ => }

      // resume all workers
      // resume message has no effect on non-paused workers
      Future
        .collect(
          workflow.getAllWorkers.map { worker =>
            send(ResumeWorker(), worker).map { ret =>
              workflow.getWorkerInfo(worker).state = ret
            }
          }.toSeq
        )
        .map { _ =>
          // update frontend status
          updateFrontendWorkflowStatus()
          enableStatusUpdate() //re-enabled it since it is disabled in pause
          actorContext.parent ! ControllerState.Running //for testing

        }
    }
  }
}
