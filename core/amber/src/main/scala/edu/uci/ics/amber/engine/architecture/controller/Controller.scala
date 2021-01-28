package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{BreakpointTriggered, ErrorOccurred, ModifyLogicCompleted, SkipTupleResponse, WorkflowCompleted, WorkflowPaused, WorkflowStatusUpdate}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ActivateLinkHandler.ActivateLink
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.OneOnEach
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.faulttolerance.materializer.{HashBasedMaterializer, OutputMaterializer}
import edu.uci.ics.amber.engine.architecture.linksemantics.{FullRoundRobin, HashBasedShuffle, LinkStrategy}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambertag.{AmberTag, LayerTag, LinkTag, OperatorIdentifier, WorkerTag, WorkflowTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants, ISourceOperatorExecutor, WorkflowLogger}
import edu.uci.ics.amber.engine.faulttolerance.scanner.HDFSFolderScanSourceOperatorExecutor
import edu.uci.ics.amber.engine.operators.OpExecConfig
import akka.actor.{Actor, ActorContext, ActorLogging, ActorPath, ActorRef, ActorSelection, Address, Cancellable, Deploy, PoisonPill, Props, Stash}
import akka.dispatch.Futures
import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.remote.RemoteScope
import akka.util.Timeout
import com.github.nscala_time.time.Imports.DateTime
import com.google.common.base.Stopwatch
import play.api.libs.json.{JsArray, JsValue, Json, __}
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
import com.softwaremill.macwire.wire
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkAck, NetworkMessage, RegisterActorRef}
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object Controller {

  def props(
      tag: WorkflowTag,
      workflow: Workflow,
      eventListener: ControllerEventListener,
      statusUpdateInterval: Long,
      parentNetworkCommunicationActorRef:ActorRef = null
  ): Props =
    Props(
      new Controller(
        tag,
        workflow,
        eventListener,
        Option.apply(statusUpdateInterval),
        parentNetworkCommunicationActorRef
      )
    )
}

class Controller(
    val tag: WorkflowTag,
    val workflow: Workflow,
    val eventListener: ControllerEventListener = ControllerEventListener(),
    val statisticsUpdateIntervalMs: Option[Long],
    parentNetworkCommunicationActorRef:ActorRef
) extends WorkflowActor(VirtualIdentity.Controller, parentNetworkCommunicationActorRef) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  val rpcHandlerInitializer = wire[ControllerAsyncRPCHandlerInitializer]

  private def errorLogAction(err: WorkflowRuntimeError): Unit = {
    eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
  }
  val controllerLogger = WorkflowLogger(s"Controller-${tag.getGlobalIdentity}-Logger")
  controllerLogger.setErrorLogAction(errorLogAction)

  var statusUpdateAskHandle: Cancellable = _

  // register controller itself
  networkCommunicationActor ! RegisterActorRef(VirtualIdentity.Controller, self)

  // build whole workflow
  workflow.build(availableNodes, networkCommunicationActor, context)

  // activate all links
  workflow.getAllLinks.foreach{
    link => asyncRPCServer.receive(ControlInvocation(-1,ActivateLink(link)), VirtualIdentity.Controller)
  }

  //for testing, report ready state to parent
  context.parent ! ControllerState.Ready

  def availableNodes: Array[Address] =
    Await
      .result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses, 5.seconds)
      .asInstanceOf[Array[Address]]


  override def receive: Receive = running

  def running:Receive = {
    acceptDirectInvocations orElse
    processControlMessages orElse{
      case other =>
        logger.logInfo(s"unhandled message: $other")
    }
  }


  def acceptDirectInvocations:Receive = {
    case invocation: ControlInvocation =>
      asyncRPCServer.receive(invocation, VirtualIdentity.Controller)
  }

}
