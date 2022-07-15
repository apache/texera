package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StoreInlinkIdsHandler.StoreInlinkIds
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

object StoreInlinkIdsHandler {

  final case class StoreInlinkIds(inlinkIds: Array[LinkIdentity]) extends ControlCommand[Unit]
}

/**
  * Used to tell the worker how many upstream links it will get the data from
  * Sender: Controller
  */
trait StoreInlinkIdsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StoreInlinkIds, sender) =>
    batchToTupleConverter.updateAllUpstreamLinkIds(msg.inlinkIds.toSet)
  }

}
