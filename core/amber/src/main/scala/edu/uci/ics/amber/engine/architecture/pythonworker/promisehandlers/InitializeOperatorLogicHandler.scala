package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
object InitializeOperatorLogicHandler {
  final case class InitializeOperatorLogic(code: String, isSource: Boolean)
      extends ControlCommand[Unit]
}
