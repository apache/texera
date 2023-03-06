package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{
  PauseType,
  UserPause,
  WorkerAsyncRPCHandlerInitializer
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, RUNNING}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ResumeHandler {
  final case class ResumeWorker() extends ControlCommand[WorkerState]
}

trait ResumeHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ResumeWorker, sender) =>
    if (stateManager.getCurrentState == PAUSED) {
      pauseManager.resume(UserPause)
      stateManager.transitTo(RUNNING)
      outputManager.adaptiveBatchingMonitor.enableAdaptiveBatching(actorContext)
    }
    stateManager.getCurrentState
  }

}
