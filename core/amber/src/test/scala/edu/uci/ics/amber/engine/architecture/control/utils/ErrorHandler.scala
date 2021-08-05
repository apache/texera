package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.architecture.control.utils.ErrorHandler.ErrorCommand
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ErrorHandler {
  case class ErrorCommand() extends ControlCommand[Unit]
}

trait ErrorHandler {
  this: TesterAsyncRPCHandlerInitializer =>

  registerHandler { (x: ErrorCommand, sender) =>
    throw new RuntimeException("this is an exception")

  }
}
