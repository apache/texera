package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

import scala.collection.mutable

object WorkerExecutionCompletedHandler {
  final case class WorkerExecutionCompleted() extends ControlCommand[Unit]
}

/** indicate a worker has completed its execution
  * i.e. received and processed all data from upstreams
  * note that this doesn't mean all the output of this worker
  * has been received by the downstream workers.
  *
  * possible sender: worker
  */
trait WorkerExecutionCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[WorkerExecutionCompleted, Unit] { (msg, sender) =>
    {
      assert(sender.isInstanceOf[ActorVirtualIdentity])

      // after worker execution is completed, query statistics immediately one last time
      // because the worker might be killed before the next query statistics interval
      // and the user sees the last update before completion
      val statsRequests = new mutable.ArrayBuffer[Future[Unit]]()
      statsRequests += execute(ControllerInitiateQueryStatistics(Option(List(sender))), CONTROLLER)

      Future
        .collect(statsRequests)
        .flatMap(_ => {
          // if entire workflow is completed, clean up
          if (cp.executionState.isCompleted) {
            // after query result come back: send completed event, cleanup ,and kill workflow
            sendToClient(WorkflowCompleted())
            cp.controllerTimerService.disableStatusUpdate()
            cp.controllerTimerService.disableMonitoring()
            cp.controllerTimerService.disableSkewHandling()
            Future.Done
          } else {
            cp.workflowScheduler
              .onWorkerCompletion(cp.workflow, cp.actorRefService, cp.actorService, sender)
              .flatMap(_ => Future.Unit)
          }
        })
    }
  }
}
