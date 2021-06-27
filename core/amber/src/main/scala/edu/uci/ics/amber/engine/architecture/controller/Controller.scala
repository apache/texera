package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.{ActorRef, Address, Cancellable, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.softwaremill.macwire.wire
import com.twitter.util.Future
import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  ErrorOccurred,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputPort
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Ready
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  VirtualIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.error.ErrorUtils.safely
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object ControllerConfig {
  def default: ControllerConfig = ControllerConfig(Option(100))
}
final case class ControllerConfig(
    statusUpdateIntervalMs: Option[Long]
)

object Controller {

  def props(
      id: WorkflowIdentity,
      workflow: Workflow,
      eventListener: ControllerEventListener,
      controllerConfig: ControllerConfig = ControllerConfig.default,
      parentNetworkCommunicationActorRef: ActorRef = null
  ): Props =
    Props(
      new Controller(
        id,
        workflow,
        eventListener,
        controllerConfig,
        parentNetworkCommunicationActorRef
      )
    )
}

class Controller(
    val id: WorkflowIdentity,
    val workflow: Workflow,
    val eventListener: ControllerEventListener = ControllerEventListener(),
    val controllerConfig: ControllerConfig,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(ActorVirtualIdentity.Controller, parentNetworkCommunicationActorRef) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayloadWithTryCatch)
  val rpcHandlerInitializer: ControllerAsyncRPCHandlerInitializer =
    wire[ControllerAsyncRPCHandlerInitializer]

  private def errorLogAction(err: WorkflowRuntimeError): Unit = {
    if (eventListener.workflowExecutionErrorListener != null) {
      eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
    }
  }

  logger.setErrorLogAction(errorLogAction)

  var statusUpdateAskHandle: Cancellable = _

  // register controller itself
  networkCommunicationActor ! RegisterActorRef(ActorVirtualIdentity.Controller, self)

  // build whole workflow
  workflow.build(availableNodes, networkCommunicationActor, context)

  // activate all links
  Future
    .collect(workflow.getAllLinks.map { link =>
      asyncRPCClient.send(
        LinkWorkers(link),
        ActorVirtualIdentity.Controller
      )
    }.toSeq)
    .onSuccess { ret =>
      workflow.getAllOperators.foreach(_.setAllWorkerState(Ready))
      if (eventListener.workflowStatusUpdateListener != null) {
        eventListener.workflowStatusUpdateListener
          .apply(WorkflowStatusUpdate(workflow.getWorkflowStatus))
      }
      // for testing, report ready state to parent
      context.parent ! ControllerState.Ready
      context.become(running)
      unstashAll()
    }

  def availableNodes: Array[Address] =
    Await
      .result(context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses, 5.seconds)
      .asInstanceOf[Array[Address]]

  override def receive: Receive = initializing

  def initializing: Receive = {
    case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload: ReturnPayload)) =>
      //process reply messages
      controlInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
    case NetworkMessage(
          id,
          WorkflowControlMessage(ActorVirtualIdentity.Controller, seqNum, payload)
        ) =>
      //process control messages from self
      controlInputPort.handleMessage(
        this.sender(),
        id,
        ActorVirtualIdentity.Controller,
        seqNum,
        payload
      )
    case _ =>
      stash() //prevent other messages to be executed until initialized
  }

  def running: Receive = {
    acceptDirectInvocations orElse {
      case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
        controlInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
      case other =>
        logger.logInfo(s"unhandled message: $other")
    }
  }

  def acceptDirectInvocations: Receive = {
    case invocation: ControlInvocation =>
      this.handleControlPayloadWithTryCatch(ActorVirtualIdentity.Controller, invocation)
  }

  def handleControlPayloadWithTryCatch(
      from: VirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    try {
      controlPayload match {
        // use control input port to pass control messages
        case invocation: ControlInvocation =>
          assert(from.isInstanceOf[ActorVirtualIdentity])
          asyncRPCServer.logControlInvocation(invocation, from)
          asyncRPCServer.receive(invocation, from.asInstanceOf[ActorVirtualIdentity])
        case ret: ReturnPayload =>
          asyncRPCClient.logControlReply(ret, from)
          asyncRPCClient.fulfillPromise(ret)
        case other =>
          logger.logError(
            WorkflowRuntimeError(
              s"unhandled control message: $other",
              "ControlInputPort",
              Map.empty
            )
          )
      }
    } catch safely {
      case e =>
        logger.logError(WorkflowRuntimeError(e, identifier.toString))
    }
  }

  override def postStop(): Unit = {
    if (statusUpdateAskHandle != null) {
      statusUpdateAskHandle.cancel()
    }
    workflow.cleanupResults()
    logger.logInfo("stopped!")
  }

}
