package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import akka.actor.PoisonPill
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.KillWorkerHandler.KillWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object KillWorkflowHandler {
  final case class KillWorkflow() extends ControlCommand[CommandCompleted]
}

trait KillWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: KillWorkflow, sender) =>
    disableStatusUpdate()
    actorContext.self ! PoisonPill
    CommandCompleted()
  }
}
