package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{EndMarker, EndOfAllMarker}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{READY, RUNNING}
import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object StartHandler {
  final case class StartWorker() extends ControlCommand[WorkerState]
}

trait StartHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StartWorker, sender) =>
    stateManager.assertState(READY)
    if (operator.isInstanceOf[ISourceOperatorExecutor]) {
      stateManager.transitTo(RUNNING)
      dataProcessor.appendElement(EndMarker)
      dataProcessor.appendElement(EndOfAllMarker)
      stateManager.getCurrentState
    } else {
      throw new WorkflowRuntimeException(
        s"non-source worker $actorId received unexpected StartWorker!"
      )
    }
  }
}
