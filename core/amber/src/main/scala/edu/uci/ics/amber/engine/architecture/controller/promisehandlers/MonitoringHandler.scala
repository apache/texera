package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.MonitoringHandler.{
  ControllerInitiateMonitoring,
  previousCallFinished
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerWorkloadInfo
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.MonitoringHandler.QuerySelfWorkloadMetrics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object MonitoringHandler {
  var previousCallFinished = true

  final case class ControllerInitiateMonitoring(
      filterByWorkers: List[ActorVirtualIdentity] = List()
  ) extends ControlCommand[Unit]
}

trait MonitoringHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler((msg: ControllerInitiateMonitoring, sender) => {
    if (!previousCallFinished) {
      Future.Done
    } else {
      previousCallFinished = false
      // send to specified workers (or all workers by default)
      val workers = workflow.getAllWorkers.filterNot(p => msg.filterByWorkers.contains(p)).toList

      // send Monitoring message
      val requests = workers.map(worker =>
        send(QuerySelfWorkloadMetrics(), worker).map(metric => {
          workflow.getOperator(worker).getWorkerWorkloadInfo(worker).dataInputWorkload =
            metric.unprocessedDataInputQueueSize + metric.stashedDataInputQueueSize
          workflow.getOperator(worker).getWorkerWorkloadInfo(worker).controlInputWorkload =
            metric.unprocessedControlInputQueueSize + metric.stashedControlInputQueueSize
        })
      )

      Future.collect(requests).onSuccess(seq => previousCallFinished = true).unit
    }
  })
}
