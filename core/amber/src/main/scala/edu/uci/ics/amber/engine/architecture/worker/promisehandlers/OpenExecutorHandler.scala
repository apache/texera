package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenExecutorHandler.OpenExecutor
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object OpenExecutorHandler {

  final case class OpenExecutor() extends ControlCommand[Unit]
}

trait OpenExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>
  registerHandler { (_: OpenExecutor, sender) =>
    dp.executor.open()
  }
}
