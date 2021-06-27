package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object QueryWorkerStatisticsHandler {
  final case class QueryWorkerStatistics() extends ControlCommand[Unit]
}

/** Get statistics from all the workers
  *
  * possible sender: controller(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: QueryWorkerStatistics, sender) =>
    {
      // send QueryStatistics message to all workers
      val requests = workflow.getAllWorkers.toList.map(worker =>
        send(QueryStatistics(), worker).map(res => (worker, res))
      )

      // wait for all workers to reply
      val allResponses = Future.collect(requests)

      // update statistics and notify frontend
      allResponses.map(responses => {
        responses.foreach(res => {
          val (worker, stats) = res
          workflow.getOperator(worker).getWorker(worker).stats = stats
        })
        updateFrontendWorkflowStatus()
      })
    }
  }
}
