package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object RetryPythonHandler {
  final case class RetryPython() extends ControlCommand[Unit]
}
