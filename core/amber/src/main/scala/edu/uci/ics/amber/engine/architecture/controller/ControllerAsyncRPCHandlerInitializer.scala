package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorContext, ActorRef, Cancellable}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.{ActivateLinkHandler, AssignBreakpointHandler, ExecutionCompletedHandler, ExecutionStartedHandler, LocalBreakpointTriggeredHandler, LocalOperatorExceptionHandler, PauseHandler, QueryWorkerStatisticsHandler, ResumeHandler, StartWorkflowHandler}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer, AsyncRPCServer}

import scala.collection.mutable

class ControllerAsyncRPCHandlerInitializer(
                                            val actorContext: ActorContext,
                                            val selfID: ActorVirtualIdentity,
                                            val controlOutputPort: ControlOutputPort,
                                            val eventListener: ControllerEventListener,
                                            val workflow: Workflow,
                                            val statusUpdateAskHandle: Cancellable,
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
with StartWorkflowHandler{

}
