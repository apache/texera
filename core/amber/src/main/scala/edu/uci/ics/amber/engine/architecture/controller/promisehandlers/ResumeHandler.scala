package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.util.VirtualIdentityUtils

/** resume the entire workflow
  *
  * possible sender: controller, client
  */
trait ResumeHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def resumeWorkflow(msg: EmptyRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    // send all workers resume
    // resume message has no effect on non-paused workers
    Future
      .collect(
        cp.workflowExecution.getRunningRegionExecutions
          .flatMap(_.getAllOperatorExecutions.map(_._2))
          .flatMap(_.getWorkerIds)
          .map { workerId =>
            workerInterface.resumeWorker(EmptyRequest(), mkContext(workerId)).map { resp =>
              cp.workflowExecution
                .getLatestOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(workerId))
                .getWorkerExecution(workerId)
                .setState(resp.state)
            }
          }
          .toSeq
      )
      .map { _ =>
        // update frontend status
        sendToClient(
          ExecutionStatsUpdate(
            cp.workflowExecution.getAllRegionExecutionsStats
          )
        )
        cp.controllerTimerService
          .enableStatusUpdate() //re-enabled it since it is disabled in pause
        EmptyReturn()
      }
  }

}
