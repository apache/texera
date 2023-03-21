package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.Props
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor._
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  NetworkInputPort,
  NetworkOutputPort,
  OutputManager
}
import edu.uci.ics.amber.engine.architecture.recovery.RecoveryQueue
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  EndMarker,
  InputEpochMarker,
  InputTuple
}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.getWorkerLogName
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      workerIndex: Int,
      workerLayer: OpExecConfig,
      parentNetworkCommunicationActorRef: NetworkSenderActorRef,
      supportFaultTolerance: Boolean
  ): Props =
    Props(
      new WorkflowWorker(
        id,
        workerIndex: Int,
        workerLayer: OpExecConfig,
        parentNetworkCommunicationActorRef,
        supportFaultTolerance
      )
    )

  def getWorkerLogName(id: ActorVirtualIdentity): String = id.name.replace("Worker:", "")
}

class WorkflowWorker(
    actorId: ActorVirtualIdentity,
    workerIndex: Int,
    workerLayer: OpExecConfig,
    parentNetworkCommunicationActorRef: NetworkSenderActorRef,
    supportFaultTolerance: Boolean
) extends WorkflowActor(actorId, parentNetworkCommunicationActorRef, supportFaultTolerance) {
  lazy val operator: IOperatorExecutor =
    workerLayer.initIOperatorExecutor((workerIndex, workerLayer))
  lazy val recoveryQueue = new RecoveryQueue(logStorage.getReader)
  lazy val upstreamLinkStatus: UpstreamLinkStatus = wire[UpstreamLinkStatus]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.actorId, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.actorId, this.handleControlPayload)
  lazy val dataOutputPort: NetworkOutputPort[DataPayload] =
    new NetworkOutputPort[DataPayload](this.actorId, this.outputDataPayload)
  lazy val outputManager: OutputManager = wire[OutputManager]
  lazy val internalQueue: WorkerInternalQueue = dataProcessor.internalQueue
  lazy val pauseManager: PauseManager = dataProcessor.pauseManager
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  val workerStateManager: WorkerStateManager = new WorkerStateManager()
  val epochManager: EpochManager = wire[EpochManager]
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  if (parentNetworkCommunicationActorRef != null) {
    parentNetworkCommunicationActorRef.waitUntil(RegisterActorRef(this.actorId, self))
  }

  override def getLogName: String = getWorkerLogName(actorId)

  def getSenderCredits(sender: ActorVirtualIdentity) = {
    internalQueue.getSenderCredits(sender)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    logger.error(s"Encountered fatal error, worker is shutting done.", reason)
    asyncRPCClient.send(
      FatalError(reason),
      CONTROLLER
    )
  }

  override def receive: Receive = {
    if (!recoveryQueue.isReplayCompleted) {
      recoveryManager.registerOnStart(() =>
        context.parent ! WorkflowRecoveryMessage(actorId, UpdateRecoveryStatus(true))
      )
      recoveryManager.registerOnEnd(() =>
        context.parent ! WorkflowRecoveryMessage(actorId, UpdateRecoveryStatus(false))
      )
      val fifoState = recoveryManager.getFIFOState(logStorage.getReader.mkLogRecordIterator())
      controlInputPort.overwriteFIFOState(fifoState)
    }
    dataProcessor.start()
    receiveAndProcessMessages
  }

  def receiveAndProcessMessages: Receive =
    acceptDirectInvocations orElse forwardResendRequest orElse disallowActorRefRelatedMessages orElse {
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

  def acceptDirectInvocations: Receive = {
    case invocation: ControlInvocation =>
      this.handleControlPayload(SELF, invocation)
  }

  def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
    dataPayload match {
      case DataFrame(payload) =>
        payload.foreach { i =>
          internalQueue.appendElement(InputTuple(from, i))
        }
      case EndOfUpstream() =>
        internalQueue.appendElement(EndMarker(from))
      case marker @ EpochMarker(_, _, _) =>
        internalQueue.appendElement(InputEpochMarker(from, marker))
      case _ =>
        throw new NotImplementedError()
    }
  }

  def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    // let dp thread process it
    controlPayload match {
      case controlCommand @ (ControlInvocation(_, _) | ReturnInvocation(_, _)) =>
        internalQueue.enqueueCommand(controlCommand, from)
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
    logManager.sendCommitted(SendRequest(to, msg))
  }

  override def postStop(): Unit = {
    // shutdown dp thread by sending a command
    val shutdown = ShutdownDPThread()
    internalQueue.enqueueCommand(
      ControlInvocation(AsyncRPCClient.IgnoreReply, shutdown),
      SELF
    )
    shutdown.completed.get()
    logger.info("stopped!")
  }

}
