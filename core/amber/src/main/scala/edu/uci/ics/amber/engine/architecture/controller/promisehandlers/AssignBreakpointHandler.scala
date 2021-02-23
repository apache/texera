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

/** Assign a breakpoint to a specific operator
  *
  * possible sender: controller, client
  */
trait AssignBreakpointHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: AssignGlobalBreakpoint[_], sender) =>
    {
      // get target operator
      val operator = workflow.getOperator(msg.operatorID)
      // attach the breakpoint
      operator.attachedBreakpoints(msg.breakpoint.id) = msg.breakpoint
      // get target workers from the operator given a breakpoint
      val targetWorkers = operator.assignBreakpoint(msg.breakpoint)
      // send AssignLocalBreakpoint message to each worker
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

}
