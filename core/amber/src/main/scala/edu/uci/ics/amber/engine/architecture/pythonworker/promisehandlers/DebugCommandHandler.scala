package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object DebugCommandHandler {
  final case class DebugCommand(cmd: String) extends ControlCommand[Unit]
}
