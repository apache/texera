package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StoreUpstreamLinkIdsHandler.StoreUpstreamLinkIds
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

object StoreUpstreamLinkIdsHandler {

  final case class StoreUpstreamLinkIds(upstreamLinkIds: Array[LinkIdentity])
      extends ControlCommand[Unit]
}

/**
  * Used to tell the worker how many upstream links it will get the data from
  * Sender: Controller
  */
trait StoreUpstreamLinkIdsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StoreUpstreamLinkIds, sender) =>
    batchToTupleConverter.updateAllUpstreamLinkIds(msg.upstreamLinkIds.toSet)
  }

}
