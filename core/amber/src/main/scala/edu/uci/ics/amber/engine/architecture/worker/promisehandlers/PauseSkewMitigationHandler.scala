package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseSkewMitigationHandler.PauseSkewMitigation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

// join-skew research related.
object PauseSkewMitigationHandler {
  final case class PauseSkewMitigation(
      skewedReceiverId: ActorVirtualIdentity,
      helperReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Boolean]
}

trait PauseSkewMitigationHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: PauseSkewMitigation, sender) =>
    tupleToBatchConverter.pauseSkewMitigation(
      cmd.skewedReceiverId,
      cmd.helperReceiverId
    )
  }
}
