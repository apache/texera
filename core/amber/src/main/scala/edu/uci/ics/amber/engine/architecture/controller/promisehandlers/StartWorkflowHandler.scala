package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running


object StartWorkflowHandler{
  final case class StartWorkflow() extends ControlCommand[CommandCompleted]
}

trait StartWorkflowHandler {
  this:ControllerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:StartWorkflow, sender) =>
      Future.collect(workflow.getStartOperators.flatMap{
        op =>
          op.getAllWorkers.map(send(StartWorker(),_))
      }.toSeq).map{
        ret =>
          println("workflow started")
          actorContext.parent ! ControllerState.Running // for testing
          workflow.getStartOperators.foreach{
            op =>
              op.setAllWorkerState(Running)
          }
          CommandCompleted()
      }
  }
}
