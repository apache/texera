package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{WorkflowCompleted, WorkflowStatusUpdate}
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.KillWorkflowHandler.KillWorkflow
import edu.uci.ics.amber.engine.architecture.principal.OperatorState
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.CollectSinkResultsHandler.CollectSinkResults
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Completed
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig

object WorkerExecutionCompletedHandler {
  final case class WorkerExecutionCompleted() extends ControlCommand[CommandCompleted]
}
// TODO: for all handlers write comment about who is sending it and how to handle it
trait WorkerExecutionCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerExecutionCompleted, sender) =>
    assert(sender.isInstanceOf[WorkerActorVirtualIdentity])
    val operator = workflow.getOperator(sender)
    val future =
      if (operator.isInstanceOf[SinkOpExecConfig]) {
        send(QueryStatistics(), sender).join(send(CollectSinkResults(), sender)).map {
          case (stats, results) =>
            val workerInfo = operator.getWorker(sender)
            workerInfo.stats = stats
            workerInfo.state = Completed
            operator.acceptResultTuples(results)
        }
      } else {
        send(QueryStatistics(), sender).map { stats =>
          val workerInfo = operator.getWorker(sender)
          workerInfo.stats = stats
          workerInfo.state = Completed
        }
      }
    future.map { ret =>
      if (eventListener.workflowStatusUpdateListener != null) {
        eventListener.workflowStatusUpdateListener
          .apply(WorkflowStatusUpdate(workflow.getWorkflowStatus))
      }
      if (workflow.isCompleted) {
        actorContext.parent ! ControllerState.Completed // for testing
        //send result to frontend
        if (eventListener.workflowCompletedListener != null) {
          eventListener.workflowCompletedListener
            .apply(
              WorkflowCompleted(
                workflow.getEndOperators.map(op => op.id.operator -> op.results).toMap
              )
            )
        }
        if (statusUpdateAskHandle != null) {
          statusUpdateAskHandle.cancel()
        }
        // clean up all workers and terminate self
        execute(KillWorkflow(), ActorVirtualIdentity.Controller)
      }
      CommandCompleted()
    }
  }

}
