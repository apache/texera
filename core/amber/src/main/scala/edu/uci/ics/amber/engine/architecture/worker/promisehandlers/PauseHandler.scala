package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.DummyInput
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{
  Completed,
  Paused,
  Pausing,
  Ready,
  Running,
  WorkerState
}
import edu.uci.ics.amber.engine.common.tuple.ITuple

object PauseHandler {

  final case class PauseWorker() extends ControlCommand[WorkerState]
}

trait PauseHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (pause: PauseWorker, sender) =>
    if (stateManager.confirmState(Running, Ready)) {
      val p = pauseManager.pause()
      stateManager.transitTo(Pausing)
      // if dp thread is blocking on waiting for input tuples:
      if (dataProcessor.isQueueEmpty) {
        // insert dummy batch to unblock dp thread
        dataProcessor.appendElement(DummyInput)
      }
      p.map { res =>
        logger.logInfo("pause actually returned")
        stateManager.transitTo(Paused)
        stateManager.getCurrentState
      }
    } else {
      Future { stateManager.getCurrentState }
    }
  }
}
