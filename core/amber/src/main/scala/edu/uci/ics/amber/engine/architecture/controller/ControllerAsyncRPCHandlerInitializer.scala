package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, Cancellable}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{WorkflowResultUpdate, WorkflowStatusUpdate}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.{ControllerInitiateQueryResults, ControllerInitiateQueryStatistics}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers._
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class ControllerAsyncRPCHandlerInitializer(
                                              val logger: WorkflowLogger,
    val actorContext: ActorContext,
    val selfID: ActorVirtualIdentity,
    val controlOutputPort: ControlOutputPort,
    val eventListener: ControllerEventListener,
    val workflow: Workflow,
    val controllerConfig: ControllerConfig,
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
    with FatalErrorHandler
    with PythonPrintHandler {

  var statusUpdateAskHandle: Option[Cancellable] = None
  var resultUpdateAskHandle: Option[Cancellable] = None

  def enableStatusUpdate(): Unit = {
    if (controllerConfig.statusUpdateIntervalMs.nonEmpty && statusUpdateAskHandle.isEmpty) {
      statusUpdateAskHandle = Option(
        actorContext.system.scheduler.scheduleAtFixedRate(
          0.milliseconds,
          FiniteDuration.apply(controllerConfig.statusUpdateIntervalMs.get, MILLISECONDS),
          actorContext.self,
          ControlInvocation(
            AsyncRPCClient.IgnoreReplyAndDoNotLog,
            ControllerInitiateQueryStatistics()
          )
        )(actorContext.dispatcher)
      )
    }
    if (controllerConfig.resultUpdateIntervalMs.nonEmpty && resultUpdateAskHandle.isEmpty) {
      resultUpdateAskHandle = Option(
        actorContext.system.scheduler.scheduleAtFixedRate(
          0.milliseconds,
          FiniteDuration.apply(controllerConfig.resultUpdateIntervalMs.get, MILLISECONDS),
          actorContext.self,
          ControlInvocation(
            AsyncRPCClient.IgnoreReplyAndDoNotLog,
            ControllerInitiateQueryResults(Option.empty)
          )
        )(actorContext.dispatcher)
      )
    }
  }

  def disableStatusUpdate(): Unit = {
    if (statusUpdateAskHandle.nonEmpty) {
      statusUpdateAskHandle.get.cancel()
      statusUpdateAskHandle = Option.empty
    }
    if (resultUpdateAskHandle.nonEmpty) {
      resultUpdateAskHandle.get.cancel()
      resultUpdateAskHandle = Option.empty
    }
  }

  def updateFrontendWorkflowStatus(): Unit = {
    if (eventListener.workflowStatusUpdateListener != null) {
      eventListener.workflowStatusUpdateListener
        .apply(WorkflowStatusUpdate(workflow.getWorkflowStatus))
    }
  }

  def updateFrontendWorkflowResult(workflowResultUpdate: WorkflowResultUpdate): Unit = {
    if (eventListener.workflowResultUpdateListener != null) {
      eventListener.workflowResultUpdateListener.apply(workflowResultUpdate)
    }
  }

}
