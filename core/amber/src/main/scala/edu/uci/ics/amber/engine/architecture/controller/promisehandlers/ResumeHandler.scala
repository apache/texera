package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running

object ResumeHandler {
  final case class ResumeWorkflow() extends ControlCommand[CommandCompleted]
}

trait ResumeHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ResumeWorkflow, sender) =>
    Future
      .collect(workflow.getAllWorkers.map { worker =>
        send(ResumeWorker(), worker)
      }.toSeq)
      .map { ret =>
        workflow.getAllOperators.foreach{
          op => op.setAllWorkerState(Running)
        }
        if (eventListener.workflowStatusUpdateListener != null) {
          eventListener.workflowStatusUpdateListener
            .apply(WorkflowStatusUpdate(workflow.getWorkflowStatus))
        }
        enableStatusUpdate() //re-enabled it since it is disabled in pause
        actorContext.parent ! ControllerState.Running //for testing
        CommandCompleted()
      }
  }
}
