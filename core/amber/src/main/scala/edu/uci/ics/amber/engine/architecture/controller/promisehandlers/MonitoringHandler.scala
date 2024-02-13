package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.MonitoringHandler.ControllerInitiateMonitoring
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.MonitoringHandler.QuerySelfWorkloadMetrics
import edu.uci.ics.amber.engine.common.{AmberConfig, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MonitoringHandler {
  final case class ControllerInitiateMonitoring(
      filterByWorkers: List[ActorVirtualIdentity] = List()
  ) extends ControlCommand[Unit]
}

trait MonitoringHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  var previousCallFinished = true

  def updateWorkloadSamples(
      collectedAt: ActorVirtualIdentity,
      allDownstreamWorkerToNewSamples: List[
        Map[ActorVirtualIdentity, List[Long]]
      ]
  ): Unit = {
    if (allDownstreamWorkerToNewSamples.isEmpty) {
      return
    }
    val existingSamples = workflowReshapeState.workloadSamples.getOrElse(
      collectedAt,
      new mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]()
    )
    for (workerToNewSamples <- allDownstreamWorkerToNewSamples) {
      for ((wid, samples) <- workerToNewSamples) {
        var existingSamplesForWorker = existingSamples.getOrElse(wid, new ArrayBuffer[Long]())
        // Remove the lowest sample as it may be incomplete
        val samplesWithoutLowest = samples.toBuffer
        samplesWithoutLowest.remove(samples.indexOf(samples.min))
        existingSamplesForWorker.appendAll(samplesWithoutLowest)

        // clean up to save memory
        val maxSamplesPerWorker = AmberConfig.reshapeMaxWorkloadSamplesInController
        if (existingSamplesForWorker.size >= maxSamplesPerWorker) {
          existingSamplesForWorker = existingSamplesForWorker.slice(
            existingSamplesForWorker.size - maxSamplesPerWorker,
            existingSamplesForWorker.size
          )
        }

        existingSamples(wid) = existingSamplesForWorker
      }
    }
    workflowReshapeState.workloadSamples(collectedAt) = existingSamples
  }

  registerHandler[ControllerInitiateMonitoring, Unit]((msg, sender) => {
    if (!previousCallFinished) {
      Future.Done
    } else {
      previousCallFinished = false
      // send to specified workers (or all workers by default)
      val workers =
        cp.workflowExecution.getAllBuiltWorkers
          .filterNot(p => msg.filterByWorkers.contains(p))
          .toList

      // send Monitoring message
      val requests = workers.map(workerId =>
        send(QuerySelfWorkloadMetrics(), workerId).map({
          case (metrics, samples) =>
            val physicalOpId = VirtualIdentityUtils.getPhysicalOpId(workerId)
            cp.workflowExecution
              .getOperatorExecution(physicalOpId)
              .get
              .getWorkerWorkloadInfo(workerId)
              .dataInputWorkload =
              metrics.unprocessedDataInputQueueSize + metrics.stashedDataInputQueueSize
            cp.workflowExecution
              .getOperatorExecution(physicalOpId)
              .get
              .getWorkerWorkloadInfo(workerId)
              .controlInputWorkload =
              metrics.unprocessedControlInputQueueSize + metrics.stashedControlInputQueueSize
            updateWorkloadSamples(workerId, samples)
        })
      )

      Future.collect(requests).onSuccess(seq => previousCallFinished = true).unit
    }
  })
}
