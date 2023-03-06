package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners.ReshapePartitioner
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.MonitoringHandler.QuerySelfWorkloadMetrics
import edu.uci.ics.amber.engine.architecture.worker.workloadmetrics.SelfWorkloadMetrics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MonitoringHandler {
  // used to get the workload metrics of a worker and the workload samples that it has collected
  // for the workers of the next operator
  final case class QuerySelfWorkloadMetrics()
      extends ControlCommand[
        (SelfWorkloadMetrics, List[Map[ActorVirtualIdentity, List[Long]]])
      ]
}

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait MonitoringHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  def getWorkloadHistory(
      outputManager: OutputManager
  ): List[Map[ActorVirtualIdentity, List[Long]]] = {
    val allDownstreamSamples =
      new ArrayBuffer[Map[ActorVirtualIdentity, List[Long]]]()
    outputManager.partitioners.values.foreach(partitioner => {
      if (partitioner.isInstanceOf[ReshapePartitioner]) {
        // Reshape only needs samples from workers that shuffle data across nodes
        allDownstreamSamples.append(
          partitioner.asInstanceOf[ReshapePartitioner].getWorkloadHistory()
        )
      }
    })
    allDownstreamSamples.toList
  }

  registerHandler { (msg: QuerySelfWorkloadMetrics, sender) =>
    try {
      val workloadMetrics = SelfWorkloadMetrics(
        internalQueue.getDataQueueLength,
        internalQueue.getControlQueueLength,
        dataInputPort.getStashedMessageCount(),
        controlInputPort.getStashedMessageCount()
      )

      val samples = getWorkloadHistory(outputManager)
      (workloadMetrics, samples)
    } catch {
      case exception: Exception =>
        (
          SelfWorkloadMetrics(-1, -1, -1, -1),
          List[Map[ActorVirtualIdentity, List[Long]]]()
        )
    }
  }

}
