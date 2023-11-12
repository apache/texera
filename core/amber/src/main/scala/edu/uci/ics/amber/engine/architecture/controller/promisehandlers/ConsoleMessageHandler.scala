package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ConsoleMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

object ConsoleMessageHandler {
  case class ConsoleMessageTriggered(consoleMessage: ConsoleMessage) extends ControlCommand[Unit]
}

trait ConsoleMessageHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: ConsoleMessageTriggered, sender) =>
    {
      if (msg.consoleMessage.msgType.isError) {
        // if its an error message, pause the workflow
        execute(PauseWorkflow(), CONTROLLER)
      }

      // forward message to frontend
      sendToClient(msg)
    }
  }
}
