package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{
  BackpressurePause,
  DataProcessorRPCHandlerInitializer
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.BackpressureHandler.Backpressure
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object BackpressureHandler {
  final case class Backpressure(enableBackpressure: Boolean) extends ControlCommand[Unit]
}

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait BackpressureHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: Backpressure, _) =>
    if (msg.enableBackpressure) {
      dp.pauseManager.pause(BackpressurePause)
    } else {
      dp.pauseManager.resume(BackpressurePause)
    }
  }

}
