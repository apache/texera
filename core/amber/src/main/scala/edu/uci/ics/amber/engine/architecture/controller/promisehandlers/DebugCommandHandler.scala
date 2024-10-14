package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, DebugCommandRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

trait DebugCommandHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def sendDebugCommand(msg:DebugCommandRequest, ctx:AsyncRPCContext):Empty = {
    getProxy.sendDebugCommand(msg, mkContext(ActorVirtualIdentity(msg.workerId)))
    Empty()
  }

}
