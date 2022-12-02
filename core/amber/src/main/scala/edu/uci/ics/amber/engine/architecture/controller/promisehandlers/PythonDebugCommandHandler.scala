package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PythonDebugCommandHandler.PythonDebugCommand
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.DebugCommandHandler.DebugCommand
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object PythonDebugCommandHandler {
  final case class PythonDebugCommand(workerId: String, cmd: String)
    extends ControlCommand[Unit]
}

trait PythonDebugCommandHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: PythonDebugCommand, sender) => {

    Future(send(DebugCommand(msg.cmd), ActorVirtualIdentity(msg.workerId))).unit
  }
  }
}
