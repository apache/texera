package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryLoadMetricsHandler.QueryLoadMetrics
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.CurrentLoadMetrics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

// join-skew research related.
object QueryLoadMetricsHandler {
  final case class QueryLoadMetrics() extends ControlCommand[CurrentLoadMetrics]
}

trait QueryLoadMetricsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { query: QueryLoadMetrics =>
    // workerStateManager.shouldBe(Running, Ready)
    CurrentLoadMetrics(
      dataProcessor.getQueueSize() / Constants.defaultBatchSize,
      dataProcessor.collectStatistics()._1 / Constants.defaultBatchSize,
      dataInputPort.getStashedMessageCount()
    )
  }
}
