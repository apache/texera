package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.RetrieveStateHandler.RetrieveState
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object RetrieveStateHandler {
  final case class RetrieveState() extends ControlCommand[Unit]
}

trait RetrieveStateHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: RetrieveState, sender) =>
    // no op for now
  }

}
