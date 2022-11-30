package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PythonDebugCommandHandler.PythonDebugCommand
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.DebugCommandHandler.DebugCommand
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object PythonDebugCommandHandler {
  final case class PythonDebugCommand(operatorId: String, workerId: String, cmd: String)
      extends ControlCommand[Unit]
}

trait PythonDebugCommandHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: PythonDebugCommand, sender) =>
    {

      Future
        .collect(
          workflow
            .getOperator(msg.operatorId)
            .getAllWorkers
            .map(worker => send(DebugCommand(msg.cmd), worker))
            .toList
        )
        .unit
    }
  }
}
