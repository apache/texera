package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StepHandler.{ContinueProcessing, StopProcessing}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object StepHandler {
  final case class ContinueProcessing(stepSize:Option[Int] = None) extends ControlCommand[Unit]
  final case class StopProcessing() extends ControlCommand[Unit]
}

trait StepHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: ContinueProcessing, sender) =>
    throw new RuntimeException("Should not explicitly handle this")
  }

  registerHandler { (msg: StopProcessing, sender) =>
    throw new RuntimeException("Should not explicitly handle this")
  }

}
