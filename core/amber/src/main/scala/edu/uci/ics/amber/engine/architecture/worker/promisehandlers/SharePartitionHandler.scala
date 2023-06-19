package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners.ReshapePartitioner
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SharePartitionHandler.SharePartition
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object SharePartitionHandler {
  final case class SharePartition(
      skewedReceiverId: ActorVirtualIdentity,
      helperReceiverId: ActorVirtualIdentity,
      tuplesToRedirectNumerator: Long,
      tuplesToRedirectDenominator: Long
  ) extends ControlCommand[Boolean]
}

trait SharePartitionHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  /**
    * Used by Reshape to share the input of skewed worker with the helper worker.
    * For every `tuplesToRedirectDenominator` tuples in the partition of the skewed
    * worker, `tuplesToRedirectNumerator` tuples will be redirected to the helper.
    */
  def sharePartition(
      outputManager: OutputManager,
      skewedReceiverId: ActorVirtualIdentity,
      helperReceiverId: ActorVirtualIdentity,
      tuplesToRedirectNumerator: Long,
      tuplesToRedirectDenominator: Long
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
          .addReceiverToBucket(
            skewedReceiverId,
            helperReceiverId,
            tuplesToRedirectNumerator,
            tuplesToRedirectDenominator
          )
        success = success | receiversFound
      }
    })
    success
  }
  registerHandler { (cmd: SharePartition, sender) =>
    sharePartition(
      outputManager,
      cmd.skewedReceiverId,
      cmd.helperReceiverId,
      cmd.tuplesToRedirectNumerator,
      cmd.tuplesToRedirectDenominator
    )
  }
}
