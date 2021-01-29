package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import akka.actor.{ActorContext, ActorPath}
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.DummyInput
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple

object PauseHandler {

  final case class PauseWorker() extends ControlCommand[CommandCompleted]
}

trait PauseHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (pause: PauseWorker, sender) =>
    // workerStateManager.shouldBe(Running, Ready)
    val p = pauseManager.pause()
    // workerStateManager.transitTo(Pausing)
    // if dp thread is blocking on waiting for input tuples:
    if (dataProcessor.isQueueEmpty) {
      // insert dummy batch to unblock dp thread
      dataProcessor.appendElement(DummyInput())
    }
    p.map { res =>
      logger.logInfo("pause actually returned")
      //workerStateManager.transitTo(Paused)
      CommandCompleted()
    }
  }
}
