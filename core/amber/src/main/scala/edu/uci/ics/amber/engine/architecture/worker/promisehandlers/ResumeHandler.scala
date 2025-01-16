package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, RUNNING}
import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, UserPause}

trait ResumeHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def resumeWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerStateResponse] = {
    if (dp.stateManager.getCurrentState == PAUSED) {
      dp.pauseManager.resume(UserPause)
      dp.stateManager.transitTo(RUNNING)
      dp.adaptiveBatchingMonitor.resumeAdaptiveBatching()
    }
    WorkerStateResponse(dp.stateManager.getCurrentState)
  }

}
