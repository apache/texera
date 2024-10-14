package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait QueryCurrentInputTupleHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: QueryCurrentInputTuple, sender) =>
    dp.inputManager.getCurrentTuple
  }
}
