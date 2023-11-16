package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorRef, Address, Stash}
import akka.pattern.ask
import akka.util.Timeout
import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  GetActorRef,
  MessageBecomesDeadLetter,
  NetworkAck,
  NetworkMessage,
  RegisterActorRef,
  CreditResponse,
  CreditRequest
}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object WorkflowActor {

  /** Ack for NetworkMessage
    *
    * @param messageId Long, id for a NetworkMessage, used for FIFO and ExactlyOnce
    */
  final case class NetworkAck(messageId: Long)

  final case class MessageBecomesDeadLetter(message: NetworkMessage)

  /** Identifier <-> ActorRef related messages
    */
  final case class GetActorRef(id: ActorVirtualIdentity, replyTo: Set[ActorRef])

  final case class RegisterActorRef(id: ActorVirtualIdentity, ref: ActorRef)

  /** All outgoing message should be eventually NetworkMessage
    *
    * @param messageId       Long, id for a NetworkMessage, used for FIFO and ExactlyOnce
    * @param internalMessage WorkflowMessage, the message payload
    */
  final case class NetworkMessage(messageId: Long, internalMessage: WorkflowFIFOMessage)

  // sent from network communicator to next worker to poll for credit information
  final case class CreditRequest(channelEndpointID: ChannelID)

  final case class CreditResponse(channelEndpointID: ChannelID, credit: Int)
}

abstract class WorkflowActor(val actorId: ActorVirtualIdentity)
    extends Actor
    with Stash
    with AmberLogging {

  //
  // Akka related components:
  //
  val actorService: AkkaActorService = new AkkaActorService(actorId, this.context)
  actorService.getAvailableNodeAddressesFunc = () => {
    implicit val timeout: Timeout = 5.seconds
    Await
      .result(
        context.actorSelection("/user/cluster-info") ? GetAvailableNodeAddresses(),
        5.seconds
      )
      .asInstanceOf[Array[Address]]
  }
  val actorRefMappingService: AkkaActorRefMappingService = new AkkaActorRefMappingService(
    actorService
  )
  actorRefMappingService.registerActorRef(actorId, self)
  val transferService: AkkaMessageTransferService =
    new AkkaMessageTransferService(actorService, actorRefMappingService, handleBackpressure)

  def receiveActorRefRelatedMessages: Receive = {
    case GetActorRef(actorId, replyTo) =>
      actorRefMappingService.retrieveActorRef(actorId, replyTo)
    case RegisterActorRef(actorId, ref) =>
      actorRefMappingService.registerActorRef(actorId, ref)
  }

  // actor behavior for FIFO messages
  def receiveMessageAndAck: Receive = {
    case NetworkMessage(id, workflowMsg @ WorkflowFIFOMessage(channel, _, _)) =>
      actorRefMappingService.registerActorRef(channel.from, sender)
      try {
        handleInputMessage(id, workflowMsg)
      } catch {
        case e: Throwable =>
          logger.warn("actor failed due to exception", e)
          throw e
      }
    case NetworkAck(id) =>
      transferService.receiveAck(id)
  }

  def receiveCreditMessages: Receive = {
    case CreditRequest(channel) =>
      sender ! CreditResponse(channel, getSenderCredits(channel))
    case CreditResponse(channel, credit) =>
      transferService.updateChannelCreditFromReceiver(channel, credit)
  }

  def receiveDeadLetterMessage: Receive = {
    case MessageBecomesDeadLetter(msg) =>
      actorRefMappingService.removeActorRef(msg.internalMessage.channel.from)
  }

  def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit

  //
  //flow control:
  //
  def getSenderCredits(channelID: ChannelID): Int

  def handleBackpressure(isBackpressured: Boolean): Unit

  //
  //Actor lifecycle: Initialization
  //
  def initState(): Unit

  override def preStart(): Unit = {
    try {
      transferService.initialize()
      initState()
    } catch {
      case t: Throwable =>
        logger.warn("actor initialization failed due to exception", t)
        throw t
    }
  }

  override def receive: Receive = {
    receiveActorRefRelatedMessages orElse
      receiveMessageAndAck orElse
      receiveCreditMessages orElse
      receiveDeadLetterMessage
  }

  override def postStop(): Unit = {
    transferService.stop()
  }

}
