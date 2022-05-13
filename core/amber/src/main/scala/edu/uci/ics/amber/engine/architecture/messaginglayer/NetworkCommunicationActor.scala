package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor._
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.BackpressureHandler.Backpressure
import edu.uci.ics.amber.engine.common
import edu.uci.ics.amber.engine.common.{AmberLogging, Constants}
import edu.uci.ics.amber.engine.common.ambermessage.{
  CreditRequest,
  WorkflowControlMessage,
  WorkflowMessage,
  WorkflowMessageType
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import scala.collection.mutable
import scala.concurrent.duration._

object NetworkCommunicationActor {

  def props(parentSender: ActorRef, actorId: ActorVirtualIdentity): Props =
    Props(new NetworkCommunicationActor(parentSender, actorId))

  /** to distinguish between main actor self ref and
    * network sender actor
    * TODO: remove this after using Akka Typed APIs
    *
    * @param ref
    */
  case class NetworkSenderActorRef(ref: ActorRef) {
    def !(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit = {
      ref ! message
    }
  }

  final case class SendRequest(id: ActorVirtualIdentity, message: WorkflowMessage)

  /** Identifier <-> ActorRef related messages
    */
  final case class GetActorRef(id: ActorVirtualIdentity, replyTo: Set[ActorRef])

  final case class RegisterActorRef(id: ActorVirtualIdentity, ref: ActorRef)

  /** All outgoing message should be eventually NetworkMessage
    *
    * @param messageId       Long, id for a NetworkMessage, used for FIFO and ExactlyOnce
    * @param internalMessage WorkflowMessage, the message payload
    */
  final case class NetworkMessage(
      messageId: Long,
      internalMessage: WorkflowMessage
  )

  /** Ack for NetworkMessage
    * note that it should NEVER be handled by the main thread
    *
    * @param messageId Long, id for a NetworkMessage, used for FIFO and ExactlyOnce
    */
  final case class NetworkAck(messageId: Long, credits: Int)

  final case class ResendMessages()

  final case class MessageBecomesDeadLetter(message: NetworkMessage)

  final case class PollForCredit(to: ActorVirtualIdentity)
}

/** This actor handles the transformation from identifier to actorRef
  * and also sends message to other actors. This is the most outer part of
  * the messaging layer.
  */
class NetworkCommunicationActor(parentRef: ActorRef, val actorId: ActorVirtualIdentity)
    extends Actor
    with AmberLogging {

  val idToActorRefs = new mutable.HashMap[ActorVirtualIdentity, ActorRef]()
  val idToCongestionControls = new mutable.HashMap[ActorVirtualIdentity, CongestionControl]()
  val queriedActorVirtualIdentities = new mutable.HashSet[ActorVirtualIdentity]()
  val messageStash = new mutable.HashMap[ActorVirtualIdentity, mutable.Queue[WorkflowMessage]]
  val messageIDToIdentity = new mutable.LongMap[ActorVirtualIdentity]
  // register timer for resending messages
  val resendHandle: Cancellable = context.system.scheduler.scheduleWithFixedDelay(
    30.seconds,
    30.seconds,
    self,
    ResendMessages
  )(context.dispatcher)

  // add parent actor into idMap
  idToActorRefs(SELF) = context.parent

  /** keeps track of every outgoing message.
    * Each message is identified by this monotonic increasing ID.
    * It's different from the sequence number and it will only
    * be used by the output gate.
    */
  var networkMessageID = 0L

  // data structures needed by flow control
  val receiverIdToDataBacklog = new mutable.HashMap[ActorVirtualIdentity, Int]()
  val receiverIdToCredits = new mutable.HashMap[ActorVirtualIdentity, Int]()
  val overloadedReceivers = new mutable.HashSet[ActorVirtualIdentity]()
  var backpressureRequestSentToMainActor = false
  var nextSeqNumForMainActor = 0L
  var receiverToCreditPollingHandle = new mutable.HashMap[ActorVirtualIdentity, Cancellable]()

  private def incrementBacklogIfDataMessage(
      id: ActorVirtualIdentity,
      msg: WorkflowMessage
  ): Unit = {
    if (msg.msgType == WorkflowMessageType.DATA_MESSAGE) {
      receiverIdToDataBacklog(id) = receiverIdToDataBacklog.getOrElseUpdate(id, 0) + 1
    }
  }

  private def decrementBacklogIfDataMessage(
      receiverId: ActorVirtualIdentity,
      msg: WorkflowMessage
  ): Unit = {
    if (msg.msgType == WorkflowMessageType.DATA_MESSAGE) {
      if (receiverIdToDataBacklog.contains(receiverId) && receiverIdToDataBacklog(receiverId) > 0) {
        receiverIdToDataBacklog(receiverId) = receiverIdToDataBacklog(receiverId) - 1
      }
    }
  }

  private def decrementCreditIfDataMessage(
      receiverId: ActorVirtualIdentity,
      msg: WorkflowMessage
  ): Unit = {
    if (msg.msgType == WorkflowMessageType.DATA_MESSAGE) {
      receiverIdToCredits(receiverId) =
        receiverIdToCredits.getOrElseUpdate(
          receiverId,
          Constants.unprocessedBatchesCreditLimitPerSender
        ) - 1
    }
  }

  private def updateCredits(
      receiverId: ActorVirtualIdentity,
      credits: Int
  ): Unit = {
    if (credits <= 0) {
      receiverIdToCredits(receiverId) = 0
      if (!receiverToCreditPollingHandle.contains(receiverId)) {
        receiverToCreditPollingHandle(receiverId) = context.system.scheduler.scheduleWithFixedDelay(
          Constants.creditPollingInitialDelayInMs.milliseconds,
          Constants.creditPollingIntervalinMs.milliseconds,
          self,
          PollForCredit(receiverId)
        )(context.dispatcher)
      }
    } else {
      if (receiverToCreditPollingHandle.contains(receiverId)) {
        receiverToCreditPollingHandle(receiverId).cancel()
      }
      receiverToCreditPollingHandle.remove(receiverId)
      receiverIdToCredits(receiverId) = credits
    }
  }

  private def sendBackpressureMessageToParent(backpressureEnable: Boolean): Unit = {
    messageIDToIdentity(networkMessageID) = actorId
    val msgToSend = NetworkMessage(
      networkMessageID,
      WorkflowControlMessage(
        actorId,
        nextSeqNumForMainActor,
        ControlInvocation(AsyncRPCClient.IgnoreReply, Backpressure(backpressureEnable)),
        WorkflowMessageType.CONTROL_MESSAGE
      )
    )
    context.parent ! msgToSend
    networkMessageID += 1
    nextSeqNumForMainActor += 1
  }

  /**
    * This is called after the network actor receives an ack. The ack has credits from the
    * receiver actor. We compare the (credits + buffer allowed in network actor) with the
    * `backlog`, and decide whether to notify the main worker to launch backpressure.
    *
    * @param receiverId
    */
  private def informParentAboutBackpressure(receiverId: ActorVirtualIdentity): Unit = {
    if (receiverId == actorId) {
      // this ack was in response to the backpressure message sent by the network actor
      // to the main actor. No need to check for backpressure here.
      return
    }
    if (
      receiverIdToCredits(
        receiverId
      ) + Constants.localSendingBufferLimitPerReceiver < receiverIdToDataBacklog.getOrElseUpdate(
        receiverId,
        0
      )
    ) {
      overloadedReceivers.add(receiverId)
      if (!backpressureRequestSentToMainActor) {
        sendBackpressureMessageToParent(true)
        backpressureRequestSentToMainActor = true
      }
    } else {
      overloadedReceivers.remove(receiverId)
      if (!overloadedReceivers.nonEmpty && backpressureRequestSentToMainActor) {
        sendBackpressureMessageToParent(false)
        backpressureRequestSentToMainActor = false
      }
    }
  }

  /** This method should always be a part of the unified WorkflowActor receiving logic.
    * 1. when an actor wants to know the actorRef of an Identifier, it replies if the mapping
    * is known, else it will ask its parent actor.
    * 2. when it receives a mapping, it adds that mapping to the state.
    */
  def findActorRefFromVirtualIdentity: Receive = {
    case GetActorRef(actorID, replyTo) =>
      if (idToActorRefs.contains(actorID)) {
        replyTo.foreach { actor =>
          actor ! RegisterActorRef(actorID, idToActorRefs(actorID))
        }
      } else if (parentRef != null) {
        parentRef ! GetActorRef(actorID, replyTo + self)
      } else {
        logger.error(s"unknown identifier: $actorID")
      }
    case RegisterActorRef(actorID, ref) =>
      registerActorRef(actorID, ref)
  }

  /** This method forward a message by using tell pattern
    * if the map from Identifier to ActorRef is known,
    * forward the message immediately,
    * otherwise it asks parent for help.
    */
  def forwardMessage(to: ActorVirtualIdentity, msg: WorkflowMessage): Unit = {
    val congestionControl = idToCongestionControls.getOrElseUpdate(to, new CongestionControl())
    val data = NetworkMessage(networkMessageID, msg)
    messageIDToIdentity(networkMessageID) = to
    if (
      congestionControl.canSend
      && (!Constants.flowControlEnabled ||
      (msg.msgType == WorkflowMessageType.DATA_MESSAGE
      && receiverIdToCredits.getOrElseUpdate(
        to,
        Constants.unprocessedBatchesCreditLimitPerSender
      ) > 0) || msg.msgType == WorkflowMessageType.CONTROL_MESSAGE)
    ) {
      congestionControl.markMessageInTransit(data)
      decrementCreditIfDataMessage(to, msg)
      sendOrGetActorRef(to, data)
    } else {
      incrementBacklogIfDataMessage(to, msg)
      congestionControl.enqueueMessage(data)
    }
    networkMessageID += 1
  }

  /** Add one mapping from Identifier to ActorRef into its state.
    * If there are unsent messages for the actor, send them.
    *
    * @param actorId ActorVirtualIdentity, virtual ID of the Actor.
    * @param ref     ActorRef, the actual reference of the Actor.
    */
  def registerActorRef(actorId: ActorVirtualIdentity, ref: ActorRef): Unit = {
    idToActorRefs(actorId) = ref
    if (messageStash.contains(actorId)) {
      val stash = messageStash(actorId)
      while (stash.nonEmpty) {
        val msg = stash.dequeue()
        decrementBacklogIfDataMessage(actorId, msg)
        forwardMessage(actorId, msg)
      }
    }
  }

  def sendMessagesAndReceiveAcks: Receive = {
    case SendRequest(id, msg) =>
      if (idToActorRefs.contains(id)) {
        forwardMessage(id, msg)
      } else {
        val stash = messageStash.getOrElseUpdate(id, new mutable.Queue[WorkflowMessage]())
        incrementBacklogIfDataMessage(id, msg)
        stash.enqueue(msg)
        fetchActorRefMappingFromParent(id)
      }
    case NetworkAck(id, credits) =>
      val actorID = messageIDToIdentity(id)
      updateCredits(actorID, credits)
      informParentAboutBackpressure(actorID)
      if (idToCongestionControls.contains(actorID)) {
        val congestionControl = idToCongestionControls(actorID)
        congestionControl.ack(id)
        congestionControl.getBufferedMessagesToSend(receiverIdToCredits(actorID)).foreach { msg =>
          congestionControl.markMessageInTransit(msg)
          decrementCreditIfDataMessage(actorID, msg.internalMessage)
          decrementBacklogIfDataMessage(actorID, msg.internalMessage)
          sendOrGetActorRef(actorID, msg)
        }
      }
    case ResendMessages =>
      queriedActorVirtualIdentities.clear()
      idToCongestionControls.foreach {
        case (actorID, ctrl) =>
          val msgsNeedResend = ctrl.getTimedOutInTransitMessages
          if (msgsNeedResend.nonEmpty) {
            logger.info(s"output channel for $actorID: ${ctrl.getStatusReport}")
          }
          msgsNeedResend.foreach { msg =>
            sendOrGetActorRef(actorID, msg)
          }
      }
    case MessageBecomesDeadLetter(msg) =>
      // only remove the mapping from id to actorRef
      // to trigger discover mechanism
      val actorID = messageIDToIdentity(msg.messageId)
      logger.warn(s"actor for $actorID might have crashed or failed")
      idToActorRefs.remove(actorID)
      if (parentRef != null) {
        fetchActorRefMappingFromParent(actorID)
      }
    case PollForCredit(to) =>
      val req = NetworkMessage(networkMessageID, CreditRequest(actorId))
      messageIDToIdentity(networkMessageID) = to
      idToActorRefs(to) ! req
      networkMessageID += 1
  }

  override def receive: Receive = {
    sendMessagesAndReceiveAcks orElse findActorRefFromVirtualIdentity
  }

  override def postStop(): Unit = {
    resendHandle.cancel()
    logger.info(s"network communication actor stopped!")
  }

  @inline
  private[this] def sendOrGetActorRef(actorID: ActorVirtualIdentity, msg: NetworkMessage): Unit = {
    if (idToActorRefs.contains(actorID)) {
      // if actorRef is found, directly send it
      //      if (
      //        actorID
      //          .toString()
      //          .contains("0bf90d9-main-1") && actorId.toString().contains("9e0059-main-1")
      //      ) {
      //        println(s"\t\t BACKLOG 2 - ${msg.dataBackLog}")
      //      }
      idToActorRefs(actorID) ! msg
    } else {
      // otherwise, we ask the parent for the actorRef.
      if (parentRef != null) {
        fetchActorRefMappingFromParent(actorID)
      }
    }
  }

  @inline
  private[this] def fetchActorRefMappingFromParent(actorID: ActorVirtualIdentity): Unit = {
    if (!queriedActorVirtualIdentities.contains(actorID)) {
      parentRef ! GetActorRef(actorID, Set(self))
      queriedActorVirtualIdentities.add(actorID)
    }
  }
}
