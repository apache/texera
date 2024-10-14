package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, ConsoleMessageTriggeredRequest, PauseWorkflowRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_SEND_PAUSE_WORKFLOW
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

trait ConsoleMessageHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def sendConsoleMessageTriggered(msg: ConsoleMessageTriggeredRequest, ctx:AsyncRPCContext): Empty = {
    if (msg.consoleMessage.msgType.isError) {
      // if its an error message, pause the workflow
      execute(ControlInvocation(METHOD_SEND_PAUSE_WORKFLOW, PauseWorkflowRequest(), ctx, 0))
    }

    // forward message to frontend
    sendToClient(msg.consoleMessage)
    Empty()
  }

}
