package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import akka.actor.PoisonPill
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.KillWorkerHandler.KillWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object KillWorkflowHandler{
  final case class KillWorkflow() extends ControlCommand[CommandCompleted]
}


trait KillWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:KillWorkflow, sender) =>
     Future.collect(workflow.getAllWorkers.map{
       worker =>
        send(KillWorker(), worker)
     }.toSeq).map{
       ret =>
       actorContext.self ! PoisonPill
       CommandCompleted()
     }
  }
}
