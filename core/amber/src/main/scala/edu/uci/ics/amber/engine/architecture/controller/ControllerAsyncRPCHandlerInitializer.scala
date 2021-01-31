package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, ActorRef, Cancellable}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.{
  AssignBreakpointHandler,
  FatalErrorHandler,
  KillWorkflowHandler,
  LinkCompletedHandler,
  LinkWorkersHandler,
  LocalBreakpointTriggeredHandler,
  LocalOperatorExceptionHandler,
  PauseHandler,
  QueryWorkerStatisticsHandler,
  ResumeHandler,
  StartWorkflowHandler,
  WorkerExecutionCompletedHandler,
  WorkerExecutionStartedHandler
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class ControllerAsyncRPCHandlerInitializer(
    val logger: WorkflowLogger,
    val actorContext: ActorContext,
    val selfID: ActorVirtualIdentity,
    val controlOutputPort: ControlOutputPort,
    val eventListener: ControllerEventListener,
    val workflow: Workflow,
    var statusUpdateAskHandle: Cancellable,
    val statisticsUpdateIntervalMs: Option[Long],
    source: AsyncRPCClient,
    receiver: AsyncRPCServer
) extends AsyncRPCHandlerInitializer(source, receiver)
    with LinkWorkersHandler
    with AssignBreakpointHandler
    with WorkerExecutionCompletedHandler
    with WorkerExecutionStartedHandler
    with LocalBreakpointTriggeredHandler
    with LocalOperatorExceptionHandler
    with PauseHandler
    with QueryWorkerStatisticsHandler
    with ResumeHandler
    with StartWorkflowHandler
    with KillWorkflowHandler
    with LinkCompletedHandler
    with FatalErrorHandler {

  def enableStatusUpdate(): Unit = {
    if (statisticsUpdateIntervalMs.isDefined && statusUpdateAskHandle == null) {
      statusUpdateAskHandle = actorContext.system.scheduler.schedule(
        0.milliseconds,
        FiniteDuration.apply(statisticsUpdateIntervalMs.get, MILLISECONDS),
        actorContext.self,
        ControlInvocation(-1, QueryWorkerStatistics())
      )(actorContext.dispatcher)
    }
  }

  def disableStatusUpdate(): Unit = {
    if (statusUpdateAskHandle != null) {
      statusUpdateAskHandle.cancel()
      statusUpdateAskHandle = null
    }
  }

  def updateFrontendWorkflowStatus(): Unit = {
    if (eventListener.workflowStatusUpdateListener != null) {
      eventListener.workflowStatusUpdateListener
        .apply(WorkflowStatusUpdate(workflow.getWorkflowStatus))
    }
  }

}
