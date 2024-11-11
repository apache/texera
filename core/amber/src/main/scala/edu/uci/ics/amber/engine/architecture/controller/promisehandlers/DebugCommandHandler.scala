package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  DebugCommandRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

trait DebugCommandHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def debugCommand(msg: DebugCommandRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    workerInterface.debugCommand(msg, mkContext(ActorVirtualIdentity(msg.workerId)))
    EmptyReturn()
  }

}
