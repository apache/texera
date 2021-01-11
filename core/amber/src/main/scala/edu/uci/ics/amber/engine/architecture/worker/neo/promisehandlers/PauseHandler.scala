package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import akka.actor.ActorContext
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.DummyInput
import edu.uci.ics.amber.engine.architecture.worker.neo.{WorkerPromiseHandlerInitializer}
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler.WorkerPause
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{ExecutionPaused, ReportState}
import edu.uci.ics.amber.engine.common.promise.{
  ControlCommand,
  PromiseCompleted,
  SynchronizedInvocation
}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._

object PauseHandler {
  final case class WorkerPause() extends ControlCommand[PromiseCompleted]
}

trait PauseHandler {
  this: WorkerPromiseHandlerInitializer =>

  registerHandler {
    case WorkerPause() =>
      // workerStateManager.shouldBe(Running, Ready)
      val (p, ctx) = createPromise[ExecutionPaused]()
      pauseManager.registerNotifyContext(ctx)
      pauseManager.pause()
      // workerStateManager.transitTo(Pausing)
      // if dp thread is blocking on waiting for input tuples:
      if (dataProcessor.isQueueEmpty) {
        // insert dummy batch to unblock dp thread
        dataProcessor.appendElement(DummyInput())
      }
      p.map { res =>
        println("pause actually returned")
        //workerStateManager.transitTo(Paused)
        returning()
      }
  }
}
