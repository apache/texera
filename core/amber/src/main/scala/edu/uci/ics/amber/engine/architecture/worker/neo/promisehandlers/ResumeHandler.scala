package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running


object ResumeHandler{
  final case class ResumeWorker() extends ControlCommand[CommandCompleted]
}

trait ResumeHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:ResumeWorker, sender) =>
      if(pauseManager.isPaused){
        pauseManager.resume()
      }
    stateManager.transitTo(Running)
    CommandCompleted()
  }

}
