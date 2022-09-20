package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RegionsTimeSlotExpiredHandler.RegionsTimeSlotExpired
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

object RegionsTimeSlotExpiredHandler {
  final case class RegionsTimeSlotExpired(regions: Set[PipelinedRegion])
      extends ControlCommand[Unit]
}

/** Indicate that the time slot for a reason is up and the execution of the regions needs to be paused
  *
  * possible sender: controller (scheduler) or self
  */
trait RegionsTimeSlotExpiredHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: RegionsTimeSlotExpired, sender) =>
    {
      val notCompletedRegions =
        msg.regions.diff(scheduler.schedulingPolicy.getCompletedRegions())

      if (notCompletedRegions.subsetOf(scheduler.schedulingPolicy.getRunningRegions())) {
        scheduler.onTimeSlotExpired(notCompletedRegions).flatMap(_ => Future.Unit)
      } else {
        if (notCompletedRegions.nonEmpty) {
          //  The regions haven't gone into running state yet. Increase the time slot expiration duration
          logger.warn("The regions' time slot expired but they are not running yet.")
          actorContext.system.scheduler.scheduleOnce(
            FiniteDuration.apply(Constants.timeSlotExpirationDurationInMs, MILLISECONDS),
            actorContext.self,
            ControlInvocation(
              AsyncRPCClient.IgnoreReplyAndDoNotLog,
              RegionsTimeSlotExpired(notCompletedRegions)
            )
          )(actorContext.dispatcher)
        }
        Future()
      }
    }
  }
}
