package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatsUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerStateUpdatedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object WorkerStateUpdatedHandler {
  final case class WorkerStateUpdated(state: WorkerState) extends ControlCommand[Unit]
}

/** indicate the state change of a worker
  *
  * possible sender: worker
  */
trait WorkerStateUpdatedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerStateUpdated, sender) =>
    {
      // set the state
      cp.workflowExecution
        .getRunningRegionExecutions.map(_.getOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(sender)))
        .foreach(operatorExecution => operatorExecution.getWorkerExecution(sender).state = msg.state)
      sendToClient(WorkflowStatsUpdate(cp.workflowExecution.getRunningRegionExecutions.flatMap(_.getStats).toMap))
    }
  }
}
