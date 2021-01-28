package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.neo.BreakpointManager.{BreakpointSnapshot, LocalBreakpointInfo}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryBreakpointsHandler.QueryBreakpoints
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Paused

object QueryBreakpointsHandler{

  final case class QueryBreakpoints(ids:Array[String]) extends ControlCommand[BreakpointSnapshot]
}


trait QueryBreakpointsHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:QueryBreakpoints, sender) =>
      stateManager.confirmState(Paused)
      breakpointManager.getBreakpoints(dataProcessor.getCurrentInputTuple, msg.ids)
  }

}
