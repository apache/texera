package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ModifyPythonLogicHandler {
  final case class ModifyPythonLogic(code: String) extends ControlCommand[Unit]
}
