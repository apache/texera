package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.worker.WorkerState
import edu.uci.ics.amber.engine.common.worker.WorkerState.{Paused, Ready, Running}

object PauseHandler {

  final case class PauseWorker() extends ControlCommand[WorkerState]
}

trait PauseHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (pause: PauseWorker, sender) =>
    if (stateManager.confirmState(Running, Ready)) {
      pauseManager.pause()
      dataProcessor.disableDataQueue()
      stateManager.transitTo(Paused)
    }
    stateManager.getCurrentState
  }
}
