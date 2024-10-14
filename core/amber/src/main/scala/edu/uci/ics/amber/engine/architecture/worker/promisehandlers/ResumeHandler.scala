package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{Empty, WorkerStateResponse}
import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, UserPause}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, RUNNING}

trait ResumeHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def resumeWorker(request: Empty, ctx: AsyncRPCContext): Future[WorkerStateResponse] = {
    if (dp.stateManager.getCurrentState == PAUSED) {
      dp.pauseManager.resume(UserPause)
      dp.stateManager.transitTo(RUNNING)
      dp.adaptiveBatchingMonitor.resumeAdaptiveBatching()
    }
    WorkerStateResponse(dp.stateManager.getCurrentState)
  }

}
