package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object ResumeHandler{
  final case class ResumeWorkflow() extends ControlCommand[CommandCompleted]
}


trait ResumeHandler {
  this:ControllerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:ResumeWorkflow, sender) =>

    Future.collect(workflow.getAllWorkers.map{
      worker =>
        send(ResumeWorker(),worker)
    }.toSeq).map{
      ret =>
        enableStatusUpdate()
        actorContext.parent ! ControllerState.Running //for testing
        CommandCompleted()
    }
  }
}
