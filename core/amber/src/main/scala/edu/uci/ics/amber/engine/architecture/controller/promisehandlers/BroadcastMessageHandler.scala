package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.BroadcastMessageHandler.BroadcastMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity

object BroadcastMessageHandler {
  case class BroadcastMessage(boardcastTo:Iterable[PhysicalOpIdentity], command:ControlCommand[_]) extends ControlCommand[Unit]
}

trait BroadcastMessageHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler[BroadcastMessage, Unit] { (msg, sender) => {
    msg.boardcastTo.foreach {
      physicalOp =>
        this.cp.workflowExecution.getAllRegionExecutions.find(x => x.hasOperatorExecution(physicalOp)).foreach {
          x =>
            x.getOperatorExecution(physicalOp).getWorkerIds.foreach {
              worker => send(msg.command, worker)
            }
        }
    }
  }
  }
}