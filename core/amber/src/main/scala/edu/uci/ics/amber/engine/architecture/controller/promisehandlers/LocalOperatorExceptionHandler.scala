package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple

object LocalOperatorExceptionHandler{
  final case class LocalOperatorException(triggeredTuple:ITuple, e:Exception) extends ControlCommand[CommandCompleted]
}

trait LocalOperatorExceptionHandler {
  this:ControllerAsyncRPCHandlerInitializer =>
  registerHandler{
    (msg:LocalOperatorException, sender) =>
    Future.collect(workflow.getAllWorkers.map(send(PauseWorker(),_)).toSeq).map{
      ret =>

      CommandCompleted()
      // worker paused
    }
  }

}
