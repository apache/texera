package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryAndRemoveBreakpointsHandler.QueryAndRemoveBreakpoints
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.PAUSED
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object QueryAndRemoveBreakpointsHandler {

  final case class QueryAndRemoveBreakpoints(ids: Array[String])
      extends ControlCommand[Array[LocalBreakpoint]]
}

trait QueryAndRemoveBreakpointsHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: QueryAndRemoveBreakpoints, sender) =>
    dp.stateManager.assertState(PAUSED)

    val ret = dp.breakpointManager.getBreakpoints(msg.ids)
    dp.breakpointManager.removeBreakpoints(msg.ids)
    ret
  }

}
