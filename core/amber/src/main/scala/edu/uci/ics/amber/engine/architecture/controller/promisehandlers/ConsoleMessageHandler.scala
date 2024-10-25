package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ConsoleMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ConsoleMessageHandler {
  case class ConsoleMessageTriggered(consoleMessage: ConsoleMessage) extends ControlCommand[Unit]
}

trait ConsoleMessageHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler[ConsoleMessageTriggered, Unit] { (msg, sender) =>
    {
      // forward message to frontend
      sendToClient(msg)
    }
  }
}
