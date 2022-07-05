package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor._
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.BackpressureHandler.Backpressure
import edu.uci.ics.amber.engine.common
import edu.uci.ics.amber.engine.common.{AmberLogging, Constants}
import edu.uci.ics.amber.engine.common.ambermessage.{
  CreditRequest,
  WorkflowControlMessage,
  WorkflowMessage
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
  final case class NetworkMessage(messageId: Long, internalMessage: WorkflowMessage)

  /** Ack for NetworkMessage
    * note that it should NEVER be handled by the main thread
    *
    * @param messageId Long, id for a NetworkMessage, used for FIFO and ExactlyOnce
    */
  final case class NetworkAck(messageId: Long, credits: Option[Int] = None)

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
  var nextSeqNumForMainActor = 0L // used to send backpressure enable/disable messages to parent.
  val flowControl = new FlowControl()

  /**
    * If credit is <=0, a regular polling service needs to be started to get credit periodically from receiver.
    * If credit > 0, then the polling service is disabled.
    */
  private def togglePollForCredits(
      receiverId: ActorVirtualIdentity,
      enablePolling: Boolean
  ): Unit = {
    if (enablePolling && !flowControl.receiverToCreditPollingHandle.contains(receiverId)) {
      flowControl.receiverToCreditPollingHandle(receiverId) =
        context.system.scheduler.scheduleWithFixedDelay(
          Constants.creditPollingInitialDelayInMs.milliseconds,
          Constants.creditPollingIntervalinMs.milliseconds,
          self,
          PollForCredit(receiverId)
        )(context.dispatcher)
    } else if (!enablePolling && flowControl.receiverToCreditPollingHandle.contains(receiverId)) {
      flowControl.receiverToCreditPollingHandle(receiverId).cancel()
      flowControl.receiverToCreditPollingHandle.remove(receiverId)
    }
  }

  private def sendBackpressureMessageToParent(backpressureEnable: Boolean): Unit = {
    messageIDToIdentity(networkMessageID) = actorId
    val msgToSend = NetworkMessage(
      networkMessageID,
      WorkflowControlMessage(
        actorId,
        nextSeqNumForMainActor,
        ControlInvocation(AsyncRPCClient.IgnoreReply, Backpressure(backpressureEnable))
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
    */
  private def informParentAboutBackpressure(receiverId: ActorVirtualIdentity): Unit = {
    if (receiverId == actorId) {
      // this ack was in response to the backpressure message sent by the network actor
      // to the main actor. No need to check for backpressure here.
      return
    }
    if (flowControl.shouldBackpressureParent(receiverId)) {
      if (!flowControl.backpressureRequestSentToMainActor) {
        sendBackpressureMessageToParent(true)
        flowControl.backpressureRequestSentToMainActor = true
      }
    } else {
      if (
        flowControl
          .getOverloadedReceivers()
          .isEmpty && flowControl.backpressureRequestSentToMainActor
      ) {
        sendBackpressureMessageToParent(false)
        flowControl.backpressureRequestSentToMainActor = false
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
    if (congestionControl.canSend) {
      congestionControl.markMessageInTransit(data)
      sendOrGetActorRef(to, data)
    } else {
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
        forwardMessage(actorId, stash.dequeue())
      }
    }
  }

  def forwardMessageFromFlowControl(
      receiverId: ActorVirtualIdentity,
      msg: WorkflowMessage
  ): Unit = {
    if (idToActorRefs.contains(receiverId)) {
      forwardMessage(receiverId, msg)
    } else {
      val stash = messageStash.getOrElseUpdate(receiverId, new mutable.Queue[WorkflowMessage]())
      stash.enqueue(msg)
      fetchActorRefMappingFromParent(receiverId)
    }
  }

  def sendMessagesAndReceiveAcks: Receive = {
    case SendRequest(id, msg) =>
      val msgToForward = flowControl.getMessageToForward(id, msg)
      if (msgToForward.nonEmpty) {
        forwardMessageFromFlowControl(id, msgToForward.get)
      }
      informParentAboutBackpressure(id) // enable backpressure if necessary
    case NetworkAck(id, credits) =>
      if (messageIDToIdentity.contains(id)) {
        val actorID = messageIDToIdentity.remove(id).get
        if (credits.nonEmpty) {
          flowControl.updateCredits(actorID, credits.get)
          informParentAboutBackpressure(actorID) // enables/disables backpressure if necessary
          togglePollForCredits(actorID, credits.get <= 0)
          flowControl
            .getMessagesToForward(actorID)
            .foreach(msg => forwardMessageFromFlowControl(actorID, msg))
        }
        if (idToCongestionControls.contains(actorID)) {
          val congestionControl = idToCongestionControls(actorID)
          congestionControl.ack(id)
          congestionControl.getBufferedMessagesToSend
            .foreach { msg =>
              congestionControl.markMessageInTransit(msg)
              sendOrGetActorRef(actorID, msg)
            }
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
