package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{
  DataProcessorRPCHandlerInitializer,
  SchedulerTimeSlotExpiredPause
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SchedulerTimeSlotEventHandler.SchedulerTimeSlotEvent
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object SchedulerTimeSlotEventHandler {
  final case class SchedulerTimeSlotEvent(timeSlotExpired: Boolean) extends ControlCommand[Unit]
}

/** Time slot start/expiration message
  *
  * possible sender: controller(by scheduler)
  */
trait SchedulerTimeSlotEventHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: SchedulerTimeSlotEvent, _) =>
    if (msg.timeSlotExpired) {
      dp.pauseManager.pause(SchedulerTimeSlotExpiredPause)
    } else {
      dp.pauseManager.resume(SchedulerTimeSlotExpiredPause)
    }
  }

}
