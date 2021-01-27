package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryNextOpLoadMetricsHandler.QueryNextOpLoadMetrics
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{
  CurrentLoadMetrics,
  FutureLoadMetrics
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

// join-skew research related.
object QueryNextOpLoadMetricsHandler {
  final case class QueryNextOpLoadMetrics() extends ControlCommand[FutureLoadMetrics]
}

trait QueryNextOpLoadMetricsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { query: QueryNextOpLoadMetrics =>
    // workerStateManager.shouldBe(Running, Ready, Pausing, Paused, Completed)
    sendToNetworkCommActor(QueryNextOpLoadMetrics())
  }
}
