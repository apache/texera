package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LocalOperatorExceptionHandler{
  final case class LocalOperatorException(triggeredTuple:ITuple, e:Exception) extends ControlCommand[CommandCompleted]
}

trait LocalOperatorExceptionHandler {
  this:ControllerAsyncRPCHandlerInitializer =>
  registerHandler {
    (msg: LocalOperatorException, sender) =>
      if (eventListener.breakpointTriggeredListener != null) {
        eventListener.breakpointTriggeredListener.apply(
          BreakpointTriggered(mutable.HashMap((sender, FaultedTuple(msg.triggeredTuple, 0)) -> Array(s"${msg.e}").to[ArrayBuffer]), workflow.getOperator(sender).tag.operator)
        )
      }
      execute(PauseWorkflow(), VirtualIdentity.Controller)
  }
}
