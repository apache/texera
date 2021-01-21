package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorLogging, ActorRef, Stash}
import com.softwaremill.macwire.wire
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkSenderActorRef,
  GetActorRef,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  ControlInputPort,
  ControlOutputPort,
  NetworkCommunicationActor
}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.error.WorkflowRuntimeError

abstract class WorkflowActor(
    val identifier: ActorVirtualIdentity,
    parentNetworkCommunicationActorRef: ActorRef
) extends Actor
    with Stash {

  protected val logger: WorkflowLogger = WorkflowLogger(s"$identifier")

  val networkCommunicationActor: NetworkSenderActorRef = NetworkSenderActorRef(
    context.actorOf(NetworkCommunicationActor.props(parentNetworkCommunicationActorRef))
  )
  lazy val controlInputPort: ControlInputPort = wire[ControlInputPort]
  lazy val controlOutputPort: ControlOutputPort = wire[ControlOutputPort]
  lazy val asyncRPCClient: AsyncRPCClient = wire[AsyncRPCClient]
  lazy val asyncRPCServer: AsyncRPCServer = wire[AsyncRPCServer]
  // this variable cannot be lazy
  // because it should be initialized with the actor itself
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer

  def disallowActorRefRelatedMessages: Receive = {
    case GetActorRef(id, replyTo) =>
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          "workflow actor should never receive get actor ref message",
          identifier.toString,
          Map.empty
        )
      )
    case RegisterActorRef(id, ref) =>
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          "workflow actor should never receive register actor ref message",
          identifier.toString,
          Map.empty
        )
      )
  }
}
