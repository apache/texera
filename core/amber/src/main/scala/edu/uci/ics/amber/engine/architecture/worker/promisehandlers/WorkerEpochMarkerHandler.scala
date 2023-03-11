package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.WorkerEpochMarkerHandler.WorkerPropagateEpochMarker
import edu.uci.ics.amber.engine.common.ambermessage.EpochMarker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object WorkerEpochMarkerHandler {

  final case class WorkerPropagateEpochMarker(epochMarker: EpochMarker) extends ControlCommand[Unit]

}

trait WorkerEpochMarkerHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerPropagateEpochMarker, sender) =>
    {
      epochManager.triggerEpochMarkerOnCompletion(msg.epochMarker)
    }
  }

}
