package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.PythonPrintTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PythonPrintHandler.PythonPrint
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object PythonPrintHandler {

  final case class PythonPrint(message: String) extends ControlCommand[Unit]
}

trait PythonPrintHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: PythonPrint, sender) =>
    {
      // report the print message to the frontend
      sendToClient(
        PythonPrintTriggered(msg.message, workflow.getOperator(sender).id.operator)
      )
    }
  }
}
