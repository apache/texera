package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, ActorRef, Cancellable}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.QueryWorkerStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.{ActivateLinkHandler, AssignBreakpointHandler, ExecutionCompletedHandler, ExecutionStartedHandler, KillWorkflowHandler, LocalBreakpointTriggeredHandler, LocalOperatorExceptionHandler, PauseHandler, QueryWorkerStatisticsHandler, ResumeHandler, StartWorkflowHandler}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer, AsyncRPCServer}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class ControllerAsyncRPCHandlerInitializer(
                                            val actorContext: ActorContext,
                                            val selfID: ActorVirtualIdentity,
                                            val controlOutputPort: ControlOutputPort,
                                            val eventListener: ControllerEventListener,
                                            val workflow: Workflow,
                                            var statusUpdateAskHandle: Cancellable,
                                            val statisticsUpdateIntervalMs:Option[Long],
                                            source: AsyncRPCClient,
                                            receiver: AsyncRPCServer
                                          ) extends AsyncRPCHandlerInitializer(source, receiver)
with ActivateLinkHandler
with AssignBreakpointHandler
with ExecutionCompletedHandler
with ExecutionStartedHandler
with LocalBreakpointTriggeredHandler
with LocalOperatorExceptionHandler
with PauseHandler
with QueryWorkerStatisticsHandler
with ResumeHandler
with StartWorkflowHandler
with KillWorkflowHandler{

  def enableStatusUpdate(): Unit ={
    if(statisticsUpdateIntervalMs.isDefined && statusUpdateAskHandle == null){
      statusUpdateAskHandle = actorContext.system.scheduler.schedule(
        0.milliseconds,
        FiniteDuration.apply(statisticsUpdateIntervalMs.get, MILLISECONDS),
        actorContext.self,
        ControlInvocation(-1,QueryWorkerStatistics())
      )(actorContext.dispatcher)
    }
  }

  def disableStatusUpdate(): Unit ={
    if(statusUpdateAskHandle != null){
      statusUpdateAskHandle.cancel()
      statusUpdateAskHandle = null
    }
  }

}
