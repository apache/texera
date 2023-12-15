package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, READY, RUNNING}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, PhysicalLinkIdentity}

object UpdateInputLinkingHandler {

  final case class UpdateInputLinking(
      identifier: ActorVirtualIdentity,
      inputLink: PhysicalLinkIdentity
  ) extends ControlCommand[Unit]
}

trait UpdateInputLinkingHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: UpdateInputLinking, sender) =>
    dp.stateManager.assertState(READY, RUNNING, PAUSED)
    dp.registerInput(msg.identifier, msg.inputLink)
    dp.upstreamLinkStatus.registerInput(msg.identifier, msg.inputLink)
  }

}
