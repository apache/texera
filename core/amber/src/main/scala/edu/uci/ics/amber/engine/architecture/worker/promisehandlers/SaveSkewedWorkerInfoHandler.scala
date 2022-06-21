package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SaveSkewedWorkerInfoHandler.SaveSkewedWorkerInfo
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.operators.sortPartitions.SortPartitionOpExec

/**
  * The skewed worker of mutable state operators like sort uses this message to notify
  * the helper worker about the corresponding skewed worker. The helper uses this info
  * to send state back to the skewed worker at the end.
  *
  * Possible sender: Skewed worker (SendImmutableStateOrNotifyHelperHandler).
  */
object SaveSkewedWorkerInfoHandler {
  final case class SaveSkewedWorkerInfo(
      skewedWorkerId: ActorVirtualIdentity
  ) extends ControlCommand[Boolean]
}

trait SaveSkewedWorkerInfoHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (cmd: SaveSkewedWorkerInfo, sender) =>
    if (dataProcessor.getOperatorExecutor().isInstanceOf[SortPartitionOpExec]) {
      dataProcessor.getOperatorExecutor().asInstanceOf[SortPartitionOpExec].skewedWorkerIdentity =
        cmd.skewedWorkerId
      Future.True
    } else {
      Future.False
    }
  }
}
