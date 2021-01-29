package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{EndMarker, EndOfAllMarker}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Ready, Running}
import edu.uci.ics.amber.error.WorkflowRuntimeError

object StartHandler{
  final case class StartWorker() extends ControlCommand[CommandCompleted]
}


trait StartHandler {
  this:WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:StartWorker, sender) =>
      stateManager.confirmState(Ready)
      if (operator.isInstanceOf[ISourceOperatorExecutor]) {
        dataProcessor.appendElement(EndMarker())
        dataProcessor.appendElement(EndOfAllMarker())
        stateManager.transitTo(Running)
        CommandCompleted()
      } else {
          throw new WorkflowRuntimeException(WorkflowRuntimeError(
            "unexpected Start message for non-source operator!",
            selfID.toString,
            Map.empty
          ))
      }
  }
}
