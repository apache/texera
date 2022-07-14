package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StoreInlinkIdsHandler.StoreInlinkIds
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object StoreInlinkIdsHandler {

  final case class StoreInlinkIds(inlinkIds: mutable.HashSet[LinkIdentity])
      extends ControlCommand[Unit]
}

trait StoreInlinkIdsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StoreInlinkIds, sender) =>
    batchToTupleConverter.allUpstreamLinkIds = msg.inlinkIds
  }

}
