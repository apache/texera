package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EpochMarkerHandler.PropagateEpochMarker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.WorkerEpochMarkerHandler.WorkerPropagateEpochMarker
import edu.uci.ics.amber.engine.common.ambermessage.EpochMarker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.LayerIdentity

object EpochMarkerHandler {

  final case class PropagateEpochMarker(destOperator: LayerIdentity, epochMarker: EpochMarker)
      extends ControlCommand[Unit]

}

trait EpochMarkerHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: PropagateEpochMarker, sender) =>
    {
      val opExecution = cp.executionState.getOperatorExecution(msg.destOperator)
      val futures = opExecution.getBuiltWorkerIds
        .map(worker => send(WorkerPropagateEpochMarker(msg.epochMarker), worker))
        .toList
      Future.collect(futures).unit
    }
  }

}
