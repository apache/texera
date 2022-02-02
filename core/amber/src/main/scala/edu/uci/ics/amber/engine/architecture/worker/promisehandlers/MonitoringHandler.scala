package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.MonitoringHandler.QuerySelfWorkloadMetrics
import edu.uci.ics.amber.engine.architecture.worker.workloadmetrics.SelfWorkloadMetrics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object MonitoringHandler {
  final case class QuerySelfWorkloadMetrics() extends ControlCommand[SelfWorkloadMetrics]
}

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait MonitoringHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: QuerySelfWorkloadMetrics, sender) =>
    try {
      SelfWorkloadMetrics(
        dataProcessor.getDataQueueLength,
        dataProcessor.getControlQueueLength,
        dataInputPort.getStashedMessageCount(),
        controlInputPort.getStashedMessageCount()
      )
    } catch {
      case exception: Exception => SelfWorkloadMetrics(-1, -1, -1, -1)
    }
  }

}
