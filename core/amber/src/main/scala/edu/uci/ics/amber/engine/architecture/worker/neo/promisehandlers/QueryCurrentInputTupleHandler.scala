package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple

object QueryCurrentInputTupleHandler{
  final case class QueryCurrentInputTuple() extends ControlCommand[ITuple]
}


trait QueryCurrentInputTupleHandler {
  this:WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:QueryCurrentInputTuple, sender) =>
      dataProcessor.getCurrentInputTuple
  }
}
