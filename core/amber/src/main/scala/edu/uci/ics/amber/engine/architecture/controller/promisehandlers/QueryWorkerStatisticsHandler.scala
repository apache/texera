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
      // send all worker QueryStatistics message
      Future
        .collect(workflow.getAllWorkers.map { worker =>
          send(QueryStatistics(), worker).map { stats =>
            // update worker stats
            workflow.getOperator(worker).getWorker(worker).stats = stats
          }
        }.toSeq)
        .map { ret =>
          // update frontend status
          updateFrontendWorkflowStatus()
        }
    }
  }
}
