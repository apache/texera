package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Ready
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, VirtualIdentity}

object UpdateInputLinkingHandler {

  final case class UpdateInputLinking(identifier: VirtualIdentity, inputLink:LinkIdentity)
      extends ControlCommand[CommandCompleted]
}

trait UpdateInputLinkingHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: UpdateInputLinking, sender) =>
    stateManager.confirmState(Ready)
    batchToTupleConverter.registerInput(msg.identifier, msg.inputLink)
    CommandCompleted()
  }

}
