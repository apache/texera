package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, Stash}
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.logging.storage.{
  DeterminantLogStorage,
  EmptyLogStorage
}
import edu.uci.ics.amber.engine.architecture.logging.LogManager
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  GetActorRef,
  NetworkSenderActorRef,
  RegisterActorRef,
  ResendFeasibility,
  SendRequest
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  NetworkCommunicationActor,
  NetworkOutputPort
}
import edu.uci.ics.amber.engine.architecture.recovery.LocalRecoveryManager
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  ResendOutputTo,
  WorkflowControlMessage
}
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

abstract class WorkflowActor(
    val actorId: ActorVirtualIdentity,
    parentNetworkCommunicationActorRef: NetworkSenderActorRef,
    supportFaultTolerance: Boolean
) extends Actor
    with Stash
    with AmberLogging {
  lazy val controlOutputPort: NetworkOutputPort[ControlPayload] =
    new NetworkOutputPort[ControlPayload](this.actorId, this.outputControlPayload)
  lazy val asyncRPCClient: AsyncRPCClient = wire[AsyncRPCClient]
  lazy val asyncRPCServer: AsyncRPCServer = wire[AsyncRPCServer]
  val networkCommunicationActor: NetworkSenderActorRef = NetworkSenderActorRef(
    // create a network communication actor on the same machine as the WorkflowActor itself
    context.actorOf(
      NetworkCommunicationActor.props(
        parentNetworkCommunicationActorRef.ref,
        actorId,
        supportFaultTolerance
      )
    )
  )
  val logStorage: DeterminantLogStorage = {
    DeterminantLogStorage.getLogStorage(supportFaultTolerance, getLogName)
  }
  val recoveryManager = new LocalRecoveryManager()
  val logManager: LogManager =
    LogManager.getLogManager(supportFaultTolerance, networkCommunicationActor)
  if (!logStorage.isLogAvailableForRead) {
    logManager.setupWriter(logStorage.getWriter)
  } else {
    logManager.setupWriter(new EmptyLogStorage().getWriter)
  }
  // this variable cannot be lazy
  // because it should be initialized with the actor itself
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer

  // Get log file name
  def getLogName: String = "worker-actor"

  def outputControlPayload(
      to: ActorVirtualIdentity,
      self: ActorVirtualIdentity,
      seqNum: Long,
      payload: ControlPayload
  ): Unit = {
    val msg = WorkflowControlMessage(self, seqNum, payload)
    logManager.sendCommitted(SendRequest(to, msg))
  }

  def forwardResendRequest: Receive = {
    case resend: ResendOutputTo =>
      networkCommunicationActor ! resend
    case ResendFeasibility(status) =>
      if (!status) {
        // this exception will be caught by the catch in receiveAndProcessMessages
        throw new WorkflowRuntimeException(s"network sender cannot resend message!")
      }
  }

  def disallowActorRefRelatedMessages: Receive = {
    case GetActorRef =>
      throw new WorkflowRuntimeException(
        "workflow actor should never receive get actor ref message"
      )

    case RegisterActorRef =>
      throw new WorkflowRuntimeException(
        "workflow actor should never receive register actor ref message"
      )
  }

}
