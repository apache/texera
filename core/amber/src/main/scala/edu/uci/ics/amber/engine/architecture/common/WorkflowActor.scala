package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorRef, Stash}
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  GetActorRef,
  NetworkSenderActorRef,
  RegisterActorRef,
  SendRequest
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  NetworkCommunicationActor,
  NetworkOutputPort
}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

abstract class WorkflowActor(
    val actorId: ActorVirtualIdentity,
    parentNetworkCommunicationActorRef: ActorRef
) extends Actor
    with Stash
    with AmberLogging {
  lazy val controlOutputPort: NetworkOutputPort[ControlPayload] =
    new NetworkOutputPort[ControlPayload](this.actorId, this.outputControlPayload)
  lazy val asyncRPCClient: AsyncRPCClient = wire[AsyncRPCClient]
  lazy val asyncRPCServer: AsyncRPCServer = wire[AsyncRPCServer]
  val networkCommunicationActor: NetworkSenderActorRef = NetworkSenderActorRef(
    // create a network communication actor on the same machine as the WorkflowActor itself
    context.actorOf(NetworkCommunicationActor.props(parentNetworkCommunicationActorRef, actorId))
  )
  // this variable cannot be lazy
  // because it should be initialized with the actor itself
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer

  def outputControlPayload(
      to: ActorVirtualIdentity,
      self: ActorVirtualIdentity,
      seqNum: Long,
      payload: ControlPayload
  ): Unit = {
    val msg = WorkflowControlMessage(self, seqNum, payload)
    networkCommunicationActor ! SendRequest(to, msg)
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
