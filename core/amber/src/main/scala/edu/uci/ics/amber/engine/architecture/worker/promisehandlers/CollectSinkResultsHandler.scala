package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.CollectSinkResultsHandler.CollectSinkResults
import edu.uci.ics.amber.engine.common.ITupleSinkOperatorExecutor
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable

object CollectSinkResultsHandler {
  final case class CollectSinkResults() extends ControlCommand[List[ITuple]]
}

trait CollectSinkResultsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: CollectSinkResults, sender) =>
    operator match {
      case processor: ITupleSinkOperatorExecutor =>
        processor.getResultTuples()
      case _ =>
        List.empty
    }
  }

}
