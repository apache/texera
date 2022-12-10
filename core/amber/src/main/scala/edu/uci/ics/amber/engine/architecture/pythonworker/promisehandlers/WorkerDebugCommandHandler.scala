package edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object WorkerDebugCommandHandler {
  final case class WorkerDebugCommand(cmd: String) extends ControlCommand[Unit]
}
