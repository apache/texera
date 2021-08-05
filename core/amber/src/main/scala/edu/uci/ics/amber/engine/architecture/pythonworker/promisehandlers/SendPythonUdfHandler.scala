package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
object SendPythonUdfHandler {
  final case class SendPythonUdf(code: String, isSource: Boolean) extends ControlCommand[Unit]
}
