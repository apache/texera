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
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Ready
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, WorkflowIdentity}
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object Controller {

  def props(
      id: WorkflowIdentity,
      workflow: Workflow,
      eventListener: ControllerEventListener,
      statusUpdateInterval: Long,
      parentNetworkCommunicationActorRef: ActorRef = null
  ): Props =
    Props(
      new Controller(
        id,
        workflow,
        eventListener,
        Option.apply(statusUpdateInterval),
        parentNetworkCommunicationActorRef
      )
    )
}

class Controller(
    val id: WorkflowIdentity,
    val workflow: Workflow,
    val eventListener: ControllerEventListener = ControllerEventListener(),
    val statisticsUpdateIntervalMs: Option[Long],
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(ActorVirtualIdentity.Controller, parentNetworkCommunicationActorRef) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  val rpcHandlerInitializer = wire[ControllerAsyncRPCHandlerInitializer]

  private def errorLogAction(err: WorkflowRuntimeError): Unit = {
    eventListener.workflowExecutionErrorListener.apply(ErrorOccurred(err))
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
    case NetworkMessage(
          id,
          cmd @ WorkflowControlMessage(from, sequenceNumber, payload: ReturnPayload)
        ) =>
      //process reply messages
      sender ! NetworkAck(id)
      handleControlMessageWithTryCatch(cmd)
    case NetworkMessage(
          id,
          cmd @ WorkflowControlMessage(ActorVirtualIdentity.Controller, sequenceNumber, payload)
        ) =>
      //process control messages from self
      sender ! NetworkAck(id)
      handleControlMessageWithTryCatch(cmd)
    case msg =>
      stash() //prevent other messages to be executed until initialized
  }

  def running: Receive = {
    acceptDirectInvocations orElse
      processControlMessages orElse {
      case other =>
        logger.logInfo(s"unhandled message: $other")
    }
  }

  def acceptDirectInvocations: Receive = {
    case invocation: ControlInvocation =>
      asyncRPCServer.receive(invocation, ActorVirtualIdentity.Controller)
  }

  override def postStop(): Unit = {
    if (statusUpdateAskHandle != null) {
      statusUpdateAskHandle.cancel()
    }
    workflow.cleanupResults()
    logger.logInfo("stopped!")
  }

}
