package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners.ReshapePartitioner
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

  /**
    * Used by Reshape to temporarily pause the mitigation if the helper worker gets
    * too overloaded.
    */
  def pauseSkewMitigation(
      outputManager: OutputManager,
      skewedReceiverId: ActorVirtualIdentity,
      helperReceiverId: ActorVirtualIdentity
  ): Boolean = {
    var success = false
    // There can be many downstream operators that this worker sends data
    // to. The `skewedReceiverId` and `helperReceiverId` correspond to just
    // one of the operators. So, as long as the workers are found and the partition
    // is shared in one of the `partiotioners`, we return success.
    outputManager.partitioners.values.foreach(partitioner => {
      if (partitioner.isInstanceOf[ReshapePartitioner]) {
        val receiversFound = partitioner
          .asInstanceOf[ReshapePartitioner]
          .removeReceiverFromBucket(
            skewedReceiverId,
            helperReceiverId
          )
        success = success | receiversFound
      }
    })
    success
  }

  registerHandler { (cmd: PauseSkewMitigation, sender) =>
    pauseSkewMitigation(
      outputManager,
      cmd.skewedReceiverId,
      cmd.helperReceiverId
    )
  }
}
