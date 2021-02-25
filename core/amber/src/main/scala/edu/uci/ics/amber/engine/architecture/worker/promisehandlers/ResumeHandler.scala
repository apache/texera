package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{
  Paused,
  Running,
  WorkerState
}

object ResumeHandler {
  final case class ResumeWorker() extends ControlCommand[WorkerState]
}

trait ResumeHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ResumeWorker, sender) =>
    if (stateManager.getCurrentState == Paused) {
      if (pauseManager.isPaused) {
        pauseManager.resume()
      }
      dataProcessor.enableDataQueue()
      stateManager.transitTo(Running)
    }
    stateManager.getCurrentState
  }

}
