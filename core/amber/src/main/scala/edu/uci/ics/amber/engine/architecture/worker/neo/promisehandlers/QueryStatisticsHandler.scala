package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerStatistics
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object QueryStatisticsHandler{
  final case class QueryStatistics() extends ControlCommand[WorkerStatistics]
}


trait QueryStatisticsHandler {
  this:WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg: QueryStatistics, sender) =>
      val (in, out) = dataProcessor.collectStatistics()
      val state = stateManager.getCurrentState
      WorkerStatistics(state, in, out)
  }

}
