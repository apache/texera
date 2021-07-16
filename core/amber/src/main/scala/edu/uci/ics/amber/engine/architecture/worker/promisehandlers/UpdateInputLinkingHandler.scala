package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.worker.WorkerState.Ready

object UpdateInputLinkingHandler {

  final case class UpdateInputLinking(identifier: ActorVirtualIdentity, inputLink: LinkIdentity)
      extends ControlCommand[CommandCompleted]
}

trait UpdateInputLinkingHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: UpdateInputLinking, sender) =>
    stateManager.assertState(Ready)
    batchToTupleConverter.registerInput(msg.identifier, msg.inputLink)
    CommandCompleted()
  }

}
