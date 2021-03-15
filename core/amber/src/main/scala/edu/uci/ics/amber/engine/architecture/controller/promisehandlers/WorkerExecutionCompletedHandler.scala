package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkflowCompleted,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ControllerState
}
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

/** indicate a worker has completed its job
  * i.e. received and processed all data from upstreams
  * note that this doesn't mean all the output of this worker
  * has been received by the downstream workers.
  *
  * possible sender: worker
  */
trait WorkerExecutionCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerExecutionCompleted, sender) =>
    {
      assert(sender.isInstanceOf[WorkerActorVirtualIdentity])
      // get the corresponding operator of this worker
      val operator = workflow.getOperator(sender)
      val future =
        if (operator.isInstanceOf[SinkOpExecConfig]) {
          // if the operator is sink, first query stats then collect results of this worker.
          send(QueryStatistics(), sender).join(send(CollectSinkResults(), sender)).map {
            case (stats, results) =>
              val workerInfo = operator.getWorker(sender)
              workerInfo.stats = stats
              workerInfo.state = stats.workerState
              operator.acceptResultTuples(results)
          }
        } else {
          // if the operator is not a sink, just query the stats
          send(QueryStatistics(), sender).map { stats =>
            val workerInfo = operator.getWorker(sender)
            workerInfo.stats = stats
            workerInfo.state = stats.workerState
          }
        }
      future.flatMap { ret =>
        updateFrontendWorkflowStatus()
        if (workflow.isCompleted) {
          //send result to frontend
          if (eventListener.workflowCompletedListener != null) {
            eventListener.workflowCompletedListener
              .apply(
                WorkflowCompleted(
                  workflow.getEndOperators.map(op => op.id.operator -> op.results).toMap
                )
              )
          }
          disableStatusUpdate()
          actorContext.parent ! ControllerState.Completed // for testing
          // clean up all workers and terminate self
          execute(KillWorkflow(), ActorVirtualIdentity.Controller)
        } else {
          Future {
            CommandCompleted()
          }
        }
      }
    }
  }
}
