package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ExecutionCompletedHandler.ExecutionCompleted
import edu.uci.ics.amber.engine.architecture.principal.OperatorState
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.CollectSinkResultsHandler.CollectSinkResults
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Completed
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig

object ExecutionCompletedHandler{
  final case class ExecutionCompleted() extends ControlCommand[CommandCompleted]
}


trait ExecutionCompletedHandler {
  this:ControllerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:ExecutionCompleted, sender) =>
      val operator = workflow.getOperator(sender)
    val future =
      if(operator.isInstanceOf[SinkOpExecConfig]){
      send(QueryStatistics(),sender).join(send(CollectSinkResults(),sender)).map{
        case (stats, results) =>
          operator.setWorkerStatistics(sender,stats)
          operator.acceptResultTuples(results)
          operator.setWorkerState(sender, Completed)
        }
      }else{
      send(QueryStatistics(),sender).map{
        stats =>
          operator.setWorkerStatistics(sender,stats)
          operator.setWorkerState(sender, Completed)
        }
      }
    future.map{
      ret =>
        if(workflow.isCompleted){
          actorContext.parent ! ControllerState.Completed // for testing
          //send result to frontend
          if (eventListener.workflowCompletedListener != null) {
            eventListener.workflowCompletedListener
              .apply(WorkflowCompleted(workflow.getEndOperators.map(op => op.tag.operator -> op.results).toMap))
          }
          //TODO: clean up all workers and terminate self
        }
      CommandCompleted()
    }
  }

}
