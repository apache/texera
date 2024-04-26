package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object UpdatePythonExecutorHandler {
  final case class UpdatePythonExecutor(code: String, isSource: Boolean)
      extends ControlCommand[Unit]
}
