package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import akka.actor.ActorContext
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.DummyInput
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler.WorkerPause
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{ExecutionPaused, ReportState}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object PauseHandler {
  final case class WorkerPause() extends ControlCommand[ExecutionPaused]
}

trait PauseHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { pause: WorkerPause =>
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
      res
    //workerStateManager.transitTo(Paused)
    }
  }
}
