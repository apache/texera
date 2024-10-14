package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, ResumeWorkflowRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils

/** resume the entire workflow
  *
  * possible sender: controller, client
  */
trait ResumeHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def sendResumeWorkflow(msg: ResumeWorkflowRequest, ctx: AsyncRPCContext): Future[Empty] = {
    // send all workers resume
    // resume message has no effect on non-paused workers
    Future
      .collect(
        cp.workflowExecution.getRunningRegionExecutions
          .flatMap(_.getAllOperatorExecutions.map(_._2))
          .flatMap(_.getWorkerIds)
          .map { workerId =>
            send(ResumeWorker(), workerId).map { state =>
              cp.workflowExecution
                .getLatestOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(workerId))
                .getWorkerExecution(workerId)
                .setState(state)
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
      }
  }

}
