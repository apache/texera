package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ModifyOperatorLogicHandler {
  final case class ModifyOperatorLogic(code: String, isSource: Boolean) extends ControlCommand[Unit]
}
