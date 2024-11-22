package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  RetryWorkflowRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

/** retry the execution of the entire workflow
  *
  * possible sender: controller, client
  */
trait RetryWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def retryWorkflow(
      msg: RetryWorkflowRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    // if it is a PythonWorker, prepare for retry
    // retry message has no effect on completed workers
    Future
      .collect(
        msg.workers
          .map(worker => workerInterface.retryCurrentTuple(EmptyRequest(), worker))
      )
      .unit

    // resume all workers
    controllerInterface.resumeWorkflow(EmptyRequest(), mkContext(CONTROLLER))
  }

}
