package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage,
  RegisterActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  NetworkInputPort,
  NetworkOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  READY,
  RUNNING,
  UNINITIALIZED
}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  CreditRequest,
  DataPayload,
  WorkflowControlMessage,
  WorkflowDataMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      parentNetworkCommunicationActorRef: ActorRef,
      allUpstreamLinkIds: Set[LinkIdentity]
  ): Props =
    Props(new WorkflowWorker(id, op, parentNetworkCommunicationActorRef, allUpstreamLinkIds))
}

class WorkflowWorker(
    actorId: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef,
    allUpstreamLinkIds: Set[LinkIdentity]
) extends WorkflowActor(actorId, parentNetworkCommunicationActorRef) {
  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.actorId, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.actorId, this.handleControlPayload)
  lazy val dataOutputPort: NetworkOutputPort[DataPayload] =
    new NetworkOutputPort[DataPayload](this.actorId, this.outputDataPayload)
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  val workerStateManager: WorkerStateManager = new WorkerStateManager()
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  val receivedFaultedTupleIds: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  var isCompleted = false

  if (parentNetworkCommunicationActorRef != null) {
    parentNetworkCommunicationActorRef ! RegisterActorRef(this.actorId, self)
  }

  workerStateManager.assertState(UNINITIALIZED)
  workerStateManager.transitTo(READY)

  def getSenderCredits(sender: ActorVirtualIdentity) = {
    tupleProducer.getSenderCredits(sender)
  }

  override def receive: Receive = receiveAndProcessMessages

  def receiveAndProcessMessages: Receive =
    try {
      disallowActorRefRelatedMessages orElse {
        case NetworkMessage(id, WorkflowDataMessage(from, seqNum, payload)) =>
          dataInputPort.handleMessage(
            this.sender(),
            getSenderCredits(from),
            id,
            from,
            seqNum,
            payload
          )
        case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
          controlInputPort.handleMessage(
            this.sender(),
            getSenderCredits(from),
            id,
            from,
            seqNum,
            payload
          )
        case NetworkMessage(id, CreditRequest(from, _)) =>
          sender ! NetworkAck(id, Some(getSenderCredits(from)))
        case other =>
          throw new WorkflowRuntimeException(s"unhandled message: $other")
      }
    } catch {
      case err: WorkflowRuntimeException =>
        logger.error(s"Encountered fatal error, worker is shutting done.", err)
        asyncRPCClient.send(
          FatalError(err),
          CONTROLLER
        )
        throw err;
    }

  def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
    if (workerStateManager.getCurrentState == READY) {
      workerStateManager.transitTo(RUNNING)
      asyncRPCClient.send(
        WorkerStateUpdated(workerStateManager.getCurrentState),
        CONTROLLER
      )
    }
    tupleProducer.processDataPayload(from, dataPayload)
  }

  def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    // let dp thread process it
    controlPayload match {
      case controlCommand @ (ControlInvocation(_, _) | ReturnInvocation(_, _)) =>
        dataProcessor.enqueueCommand(controlCommand, from)
      case _ =>
        throw new WorkflowRuntimeException(s"unhandled control payload: $controlPayload")
    }
  }

  def outputDataPayload(
      to: ActorVirtualIdentity,
      self: ActorVirtualIdentity,
      seqNum: Long,
      payload: DataPayload
  ): Unit = {
    val msg = WorkflowDataMessage(self, seqNum, payload)
    networkCommunicationActor ! SendRequest(to, msg)
  }

  override def postStop(): Unit = {
    // shutdown dp thread by sending a command
    dataProcessor.enqueueCommand(
      ControlInvocation(AsyncRPCClient.IgnoreReply, ShutdownDPThread()),
      SELF
    )

    logger.info("stopped!")
  }

}
