package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RegionsTimeSlotExpiredHandler.RegionsTimeSlotExpired
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object RegionsTimeSlotExpiredHandler {
  final case class RegionsTimeSlotExpired(regions: Set[PipelinedRegion])
      extends ControlCommand[Unit]
}

/** Indicate that the time slot for a reason is up and the execution of the regions needs to be paused
  *
  * possible sender: controller (scheduler)
  */
trait RegionsTimeSlotExpiredHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: RegionsTimeSlotExpired, sender) =>
    {
      val notCompletedRegions =
        msg.regions.diff(cp.workflowScheduler.schedulingPolicy.getCompletedRegions)

      if (notCompletedRegions.subsetOf(cp.workflowScheduler.schedulingPolicy.getRunningRegions)) {
        cp.workflowScheduler
          .onTimeSlotExpired(
            cp.workflow,
            notCompletedRegions,
            cp.actorRefService,
            cp.actorService
          )
          .flatMap(_ => Future.Unit)
      } else {
        if (notCompletedRegions.nonEmpty) {
          // This shouldn't happen because the timer starts only after the regions have started
          // running. The only other possibility is that the region has completed which we have
          // checked above.
          throw new WorkflowRuntimeException(
            "The regions' time slot expired but they are not running yet."
          )
        }
        Future(())
      }
    }
  }
}
