package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running

object ResumeHandler {
  final case class ResumeWorker() extends ControlCommand[CommandCompleted]
}

trait ResumeHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ResumeWorker, sender) =>
    if (pauseManager.isPaused) {
      pauseManager.resume()
    }
    stateManager.transitTo(Running)
    CommandCompleted()
  }

}
