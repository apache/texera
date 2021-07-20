package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.worker.WorkerState.Ready

object AddPartitioningHandler {
  final case class AddPartitioning(tag: LinkIdentity, partitioning: Partitioning)
      extends ControlCommand[Unit]
}

trait AddPartitioningHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: AddPartitioning, sender) =>
    stateManager.assertState(Ready)
    tupleToBatchConverter.addPartitionerWithPartitioning(msg.tag, msg.partitioning)

  }

}
