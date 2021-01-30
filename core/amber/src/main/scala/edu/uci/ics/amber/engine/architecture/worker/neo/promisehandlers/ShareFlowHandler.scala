package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.ShareFlowHandler.ShareFlow
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.CurrentLoadMetrics
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

// join-skew research related.
object ShareFlowHandler {
  final case class ShareFlow(
      skewedReceiverId: ActorVirtualIdentity,
      freeReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Unit]
}

trait ShareFlowHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { cmd: ShareFlow =>
    // workerStateManager.shouldBe(Running, Ready)
    tupleToBatchConverter.changeFlow(cmd.skewedReceiverId, cmd.freeReceiverId)
  }
}
