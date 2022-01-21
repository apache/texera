package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

import scala.collection.mutable

object LocalOperatorExceptionHandler {
  final case class LocalOperatorException(triggeredTuple: ITuple, e: Throwable)
      extends ControlCommand[Unit]
}

/** indicate an exception thrown from the operator logic on a worker
  * we catch exception when calling:
  * 1. operator.processTuple
  * 2. operator.hasNext
  * 3. operator.Next
  * The triggeredTuple of this message will always be the current input tuple
  * note that this message will be sent for each faulted input tuple, so the frontend
  * need to update incrementally since there can be multiple faulted tuple
  * from different workers at the same time.
  *
  * possible sender: worker
  */
trait LocalOperatorExceptionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>
  registerHandler { (msg: LocalOperatorException, sender) =>
    {

      // get the operator where the worker caught the local operator exception
      val operator = workflow.getOperator(sender)
      operator.caughtLocalExceptions.put(sender, msg.e)

      // then pause the workflow
      execute(PauseWorkflow(), CONTROLLER)

      // report the faulted tuple to the frontend with the exception
      sendToClient(
        BreakpointTriggered(
          mutable.HashMap(
            (sender, FaultedTuple(msg.triggeredTuple, 0)) -> Array(
              msg.e.toString
            )
          ),
          workflow.getOperator(sender).id.operator
        )
      )
    }
  }
}
