package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignLocalBreakpointHandler.AssignLocalBreakpoint
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity

object AssignBreakpointHandler {
  final case class AssignGlobalBreakpoint[T](
      breakpoint: GlobalBreakpoint[T],
      operatorID: OperatorIdentity
  ) extends ControlCommand[CommandCompleted]
}

trait AssignBreakpointHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: AssignGlobalBreakpoint[_], sender) =>
    val operator = workflow.getOperator(msg.operatorID)
    operator.attachedBreakpoints(msg.breakpoint.id) = msg.breakpoint
    val targetWorkers = operator.assignBreakpoint(msg.breakpoint)
    Future
      .collect(
        msg.breakpoint
          .partition(targetWorkers)
          .map {
            case (identity, breakpoint) =>
              send(AssignLocalBreakpoint(breakpoint), identity)
          }
          .toSeq
      )
      .map { ret =>
        CommandCompleted()
      }
  }

}
