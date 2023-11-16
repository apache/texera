package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object OpenOperatorHandler {

  final case class OpenOperator() extends ControlCommand[Unit]
}

trait OpenOperatorHandler {
  this: DataProcessorRPCHandlerInitializer =>
  registerHandler { (openOperator: OpenOperator, sender) =>
    dp.operator.open()
  }
}
