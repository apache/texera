package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ConsoleMessageTriggeredRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn

trait ConsoleMessageHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def consoleMessageTriggered(
      msg: ConsoleMessageTriggeredRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // forward message to frontend
    sendToClient(msg.consoleMessage)
    EmptyReturn()
  }

}
