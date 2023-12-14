package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Actor, ActorRef, Address, Stash}
import akka.pattern.ask
import akka.util.Timeout
import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  CreditResponse,
  GetActorRef,
  MessageBecomesDeadLetter,
  NetworkAck,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.TriggerSend
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object WorkflowActor {

  /** Ack for NetworkMessage
    *
    * @param messageId Long, id of the received network message
    * @param ackedCredit Long, received size of the message, used to subtract sender's inflight credit
    * @param queuedCredit Long, receiver queue's size
    */
  final case class NetworkAck(messageId: Long, ackedCredit: Long, queuedCredit: Long)

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

  final case class CreditResponse(channelEndpointID: ChannelID, credit: Long)
}

abstract class WorkflowActor(logStorageType: String, val actorId: ActorVirtualIdentity)
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

  val logStorage: ReplayLogStorage =
    ReplayLogStorage.getLogStorage(None)
  val logManager: ReplayLogManager =
    ReplayLogManager.createLogManager(logStorage, getLogName, sendMessageFromLogWriterToActor)

  def getLogName: String = actorId.name.replace("Worker:", "")

  def sendMessageFromLogWriterToActor(msg: WorkflowFIFOMessage): Unit = {
    // limitation: TriggerSend will be processed after input messages before it.
    self ! TriggerSend(msg)
  }

  def handleTriggerSend: Receive = {
    case TriggerSend(msg) =>
      transferService.send(msg)
  }

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
    case NetworkAck(id, ackedCredit, queuedCredit) =>
      transferService.receiveAck(id, ackedCredit, queuedCredit)
  }

  def receiveCreditMessages: Receive = {
    case CreditRequest(channel) =>
      sender ! CreditResponse(channel, getQueuedCredit(channel))
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
  def getQueuedCredit(channelID: ChannelID): Long

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
      handleTriggerSend orElse
      receiveMessageAndAck orElse
      receiveCreditMessages orElse
      receiveDeadLetterMessage
  }

  override def postStop(): Unit = {
    transferService.stop()
  }

}
