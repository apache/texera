package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{
  SchedulerTimeSlotExpiredPause,
  WorkerAsyncRPCHandlerInitializer
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
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: SchedulerTimeSlotEvent, _) =>
    if (msg.timeSlotExpired) {
      pauseManager.pause(SchedulerTimeSlotExpiredPause)
    } else {
      pauseManager.resume(SchedulerTimeSlotExpiredPause)
    }
  }

}
