package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DebugCommandHandler.DebugCommand
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.WorkerDebugCommandHandler.WorkerDebugCommand
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object DebugCommandHandler {
  final case class DebugCommand(workerId: String, cmd: String) extends ControlCommand[Unit]
}

trait DebugCommandHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler[DebugCommand, Unit] { (msg, sender) =>
    {
      send(WorkerDebugCommand(msg.cmd), ActorVirtualIdentity(msg.workerId))
    }
  }
}
