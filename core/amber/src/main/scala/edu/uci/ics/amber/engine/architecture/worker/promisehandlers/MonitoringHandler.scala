package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

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
        (SelfWorkloadMetrics, ArrayBuffer[mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]])
      ]
}

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait MonitoringHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: QuerySelfWorkloadMetrics, sender) =>
    try {
      val workloadMetrics = SelfWorkloadMetrics(
        dataProcessor.getDataQueueLength,
        dataProcessor.getControlQueueLength,
        dataInputPort.getStashedMessageCount(),
        controlInputPort.getStashedMessageCount()
      )
      val samples = tupleToBatchConverter.getWorkloadHistory()
      (workloadMetrics, samples)
    } catch {
      case exception: Exception =>
        (
          SelfWorkloadMetrics(-1, -1, -1, -1),
          new ArrayBuffer[mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[Long]]]()
        )
    }
  }

}
