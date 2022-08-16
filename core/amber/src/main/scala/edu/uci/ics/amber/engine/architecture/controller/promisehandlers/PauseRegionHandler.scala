package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseRegionHandler.PauseRegion
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object PauseRegionHandler {
  final case class PauseRegion(regions: Set[PipelinedRegion]) extends ControlCommand[Unit]
}

/** Indicate a fatal error has occurred in the workflow
  *
  * possible sender: controller, worker
  */
trait PauseRegionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: PauseRegion, sender) =>
    {
      scheduler.recordTimeFinished(msg.regions)
    }
  }
}
