package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerAsyncRPCHandlerInitializer,
  ControllerState
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Running
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.operators.OpExecConfig

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

object StartWorkflowHandler {
  final case class StartWorkflow() extends ControlCommand[CommandCompleted]
}

trait StartWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StartWorkflow, sender) =>
    val startedOperators = mutable.HashSet[OperatorIdentity]()
    Future
      .collect(
        workflow.getSourceLayers
          .filter(layer => layer.canStart)
          .flatMap { layer =>
            startedOperators.add(layer.id.toOperatorIdentity)
            layer.workers.keys.map(send(StartWorker(), _))
          }
          .toSeq
      )
      .map { ret =>
        println("workflow started")
        actorContext.parent ! ControllerState.Running // for testing
        startedOperators.foreach { op =>
          workflow.getOperator(op).setAllWorkerState(Running)
        }
        enableStatusUpdate()
        CommandCompleted()
      }
  }
}
