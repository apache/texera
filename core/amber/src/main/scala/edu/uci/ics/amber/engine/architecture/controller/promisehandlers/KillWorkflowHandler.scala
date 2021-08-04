package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import akka.actor.{Kill, PoisonPill}
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object KillWorkflowHandler {
  final case class KillWorkflow() extends ControlCommand[Unit]
}

/** Kill the workflow and release all resources
  *
  * possible sender: controller, client
  */
trait KillWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: KillWorkflow, sender) =>
    {
      disableStatusUpdate()
      updateFrontendWorkflowStatus()
      // kill the controller by sending poison pill
      // the workers and network communication actors will also be killed
      // the dp thread will be shut down when the workers kill themselves
      actorContext.self ! PoisonPill
    }
  }
}
