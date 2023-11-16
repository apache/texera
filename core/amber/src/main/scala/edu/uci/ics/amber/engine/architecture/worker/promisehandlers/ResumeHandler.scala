package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, UserPause}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, RUNNING}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ResumeHandler {
  final case class ResumeWorker() extends ControlCommand[WorkerState]
}

trait ResumeHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: ResumeWorker, sender) =>
    if (dp.stateManager.getCurrentState == PAUSED) {
      dp.pauseManager.resume(UserPause)
      dp.stateManager.transitTo(RUNNING)
      dp.adaptiveBatchingMonitor.resumeAdaptiveBatching()
    }
    dp.stateManager.getCurrentState
  }

}
