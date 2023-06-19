package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ModifyPythonOperatorLogicHandler {
  final case class ModifyPythonOperatorLogic(code: String, isSource: Boolean)
      extends ControlCommand[Unit]
}
