package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Ready

object UpdateInputLinkingHandler{

  final case class UpdateInputLinking(identifier:VirtualIdentity, inputNum:Int) extends ControlCommand[CommandCompleted]
}


trait UpdateInputLinkingHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:UpdateInputLinking, sender) =>
      stateManager.confirmState(Ready)
      batchToTupleConverter.registerInput(msg.identifier, msg.inputNum)
      CommandCompleted()
  }

}
