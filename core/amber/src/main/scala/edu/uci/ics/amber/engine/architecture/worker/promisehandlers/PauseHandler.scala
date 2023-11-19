package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, UserPause}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, READY, RUNNING}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object PauseHandler {

  final case class PauseWorker() extends ControlCommand[WorkerState]
}

trait PauseHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (pause: PauseWorker, sender) =>
    if (dp.stateManager.confirmState(RUNNING, READY)) {
      dp.pauseManager.pause(UserPause)
      dp.stateManager.transitTo(PAUSED)
    }
    dp.stateManager.getCurrentState
  }
}
