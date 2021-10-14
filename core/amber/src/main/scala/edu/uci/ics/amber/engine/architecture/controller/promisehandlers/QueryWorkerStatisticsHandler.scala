package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkflowResultUpdate,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.{
  ControllerInitiateQueryResults,
  ControllerInitiateQueryStatistics
}
import edu.uci.ics.amber.engine.architecture.principal.OperatorResult
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.{
  QueryStatistics,
  QueryWorkerResult
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object QueryWorkerStatisticsHandler {

  final case class ControllerInitiateQueryStatistics(
      filterByWorkers: Option[List[ActorVirtualIdentity]] = None
  ) extends ControlCommand[Unit]

  // ask the controller to initiate querying worker results
  // optionally specify the workers to query, None indicates querying all sink workers
  final case class ControllerInitiateQueryResults(
      filterByWorkers: Option[List[ActorVirtualIdentity]] = None
  ) extends ControlCommand[Map[String, OperatorResult]]
}

/** Get statistics from all the workers
  *
  * possible sender: controller(by statusUpdateAskHandle)
  */
trait QueryWorkerStatisticsHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler((msg: ControllerInitiateQueryStatistics, sender) => {
    // send to specified workers (or all workers by default)
    val workers = msg.filterByWorkers.getOrElse(workflow.getAllWorkers).toList

    // send QueryStatistics message
    val requests = workers.map(worker =>
      // must immediately update worker state and stats after reply
      send(QueryStatistics(), worker).map(res => {
        workflow.getOperator(worker).getWorker(worker).state = res.workerState
        workflow.getOperator(worker).getWorker(worker).stats = res
      })
    )

    // wait for all workers to reply before notifying frontend
    Future
      .collect(requests)
      .map(_ => sendToClient(WorkflowStatusUpdate(workflow.getWorkflowStatus)))
  })

  registerHandler((msg: ControllerInitiateQueryResults, sender) => {
    val sinkWorkers = workflow.getSinkLayers.flatMap(l => l.workers.keys).toList
    val workers = msg.filterByWorkers.getOrElse(sinkWorkers)

    // send all sink worker QueryResult message
    val requests = workers.map(worker => {
      send(QueryWorkerResult(), worker).map(res => (worker, res))
    })

    // wait for all workers to reply, accumulate response from all workers
    val allResponses = Future.collect(requests)

    allResponses
      .map(responses => {
        // combine results of all workers to a single result list of this operator
        val operatorResultUpdate = new mutable.HashMap[String, OperatorResult]()
        responses
          .groupBy(workerResult => workflow.getOperator(workerResult._1).id)
          .foreach(operatorResult => {
            // filter out all Option.Empty from worker result response
            val workerResultList = operatorResult._2.flatMap(r => r._2)
            // construct operator result if list is not empty
            if (workerResultList.nonEmpty) {
              val operatorID = operatorResult._1.operator
              val outputMode = workerResultList.head.outputMode
              val workerResultUnion = workerResultList.flatMap(r => r.result).toList
              operatorResultUpdate(operatorID) = OperatorResult(outputMode, workerResultUnion)
            }
          })
        // send update result to frontend
        if (operatorResultUpdate.nonEmpty) {
          sendToClient(WorkflowResultUpdate(operatorResultUpdate.toMap))
        }
        operatorResultUpdate.toMap
      })
  })
}
