package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.ExecutionStatsUpdate
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
      val physicalOpId = VirtualIdentityUtils.getPhysicalOpId(sender)
      // set the state
      cp.workflowExecution.getRunningRegionExecutions
        .find(_.hasOperatorExecution(physicalOpId))
        .map(_.getOperatorExecution(physicalOpId))
        .foreach(operatorExecution =>
          operatorExecution.getWorkerExecution(sender).setState(msg.state)
        )
      sendToClient(
        ExecutionStatsUpdate(
          cp.workflowExecution.getRunningRegionExecutions.flatMap(_.getStats).toMap
        )
      )
    }
  }
}
