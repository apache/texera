package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignLocalBreakpointHandler.AssignLocalBreakpoint
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}

object AssignBreakpointHandler {
  final case class AssignGlobalBreakpoint[T](
      breakpoint: GlobalBreakpoint[T],
      operatorID: String
  ) extends ControlCommand[List[ActorVirtualIdentity]]
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
      val operatorId = new OperatorIdentity(msg.operatorID)
      val operators = cp.workflow.physicalPlan.layersOfLogicalOperator(operatorId)

      // get the last operator (output of the operator)
      val operator = operators.last
      val opExecution = cp.executionState.getOperatorExecution(operator.id)
      // attach the breakpoint
      opExecution.attachedBreakpoints(msg.breakpoint.id) = msg.breakpoint
      // get target workers from the operator given a breakpoint
      val targetWorkers = opExecution.assignBreakpoint(msg.breakpoint)

      val workersTobeAssigned: List[(ActorVirtualIdentity, LocalBreakpoint)] =
        msg.breakpoint.partition(targetWorkers).toList

      // send AssignLocalBreakpoint message to each worker
      Future
        .collect(workersTobeAssigned map {
          case (workerId, breakpoint) => send(AssignLocalBreakpoint(breakpoint), workerId)
        })
        // return workerIds to caller
        .map(_ => workersTobeAssigned.map({ case (workerId, _) => workerId }))
    }
  }

}
