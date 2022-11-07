package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.pattern.ask
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.pattern.StatusReply.Ack
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor._
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.BackpressureHandler.Backpressure
import edu.uci.ics.amber.engine.common.{AmberLogging, AmberUtils, Constants}
import edu.uci.ics.amber.engine.common.ambermessage.{
  CreditRequest,
  ResendOutputTo,
  WorkflowControlMessage,
  WorkflowMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import scala.collection.mutable
import scala.concurrent.Await
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
  case class NetworkSenderActorRef(ref: ActorRef = null) {
    def !(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit = {
      if (ref != null) {
        ref ! message
      }
    }
    def waitUntil(message: Any)(implicit sender: ActorRef = Actor.noSender): Unit = {
      implicit val timeout: Timeout = 3.seconds
      if (ref != null) {
        Await.result(ref ? message, 5.seconds)
      }
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

  final case class ResendFeasibility(isOk: Boolean)
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
  var sentMessages = new mutable.HashMap[ActorVirtualIdentity, mutable.Queue[WorkflowMessage]]
  val resendForRecoveryQueueLimit: Long =
    AmberUtils.amberConfig.getLong("fault-tolerance.max-supported-resend-queue-length")
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
      sender ! Ack
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
        val receiverId = messageIDToIdentity.remove(id).get
        if (credits.nonEmpty) {
          flowControl.updateCredits(receiverId, credits.get)
          informParentAboutBackpressure(receiverId) // enables/disables backpressure if necessary
          togglePollForCredits(receiverId, credits.get <= 0)
          flowControl
            .getMessagesToForward(receiverId)
            .foreach(msg => forwardMessageFromFlowControl(receiverId, msg))
        }
        if (idToCongestionControls.contains(receiverId)) {
          val congestionControl = idToCongestionControls(receiverId)
          val msgSent = congestionControl.ack(id)
          if (msgSent.isDefined) {
            if (sentMessages != null) {
              sentMessages
                .getOrElseUpdate(receiverId, new mutable.Queue[WorkflowMessage]())
                .enqueue(msgSent.get.internalMessage)
              if (sentMessages(receiverId).size == resendForRecoveryQueueLimit) {
                sentMessages = null //invalidate recovery
              }
            }
          }
          congestionControl.getBufferedMessagesToSend
            .foreach { msg =>
              congestionControl.markMessageInTransit(msg)
              sendOrGetActorRef(receiverId, msg)
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
    case ResendOutputTo(dest, ref) =>
      logger.info("received resend request to " + dest)
      sender ! ResendFeasibility(sentMessages != null)
      // if the output can be resent
      if (sentMessages != null) {
        // reset actor mapping
        queriedActorVirtualIdentities.remove(dest)
        idToActorRefs(dest) = ref
        // temporally block main actor
        sendBackpressureMessageToParent(true)
        // resend previous output, make sure every message is received
        if (sentMessages.contains(dest)) {
          sentMessages(dest).foreach { message =>
            ref ! NetworkMessage(-1, message)
          }
        }
        // resend message in congestion control
        if (idToCongestionControls.contains(dest)) {
          idToCongestionControls(dest).getInTransitMessages.foreach { message =>
            ref ! message
          }
        }
        // unblock main actor
        sendBackpressureMessageToParent(false)
      }
    case MessageBecomesDeadLetter(msg) =>
      // only remove the mapping from id to actorRef
      // to trigger discover mechanism
      if (messageIDToIdentity.contains(msg.messageId)) {
        val actorID = messageIDToIdentity(msg.messageId)
        logger.warn(s"actor for $actorID might have crashed or failed")
        if (parentRef != null) {
          fetchActorRefMappingFromParent(actorID)
        }
      } else {
        logger.warn("message: " + msg + " get lost but we don't know where it is sent to.")
      }
    case PollForCredit(to) =>
      if (idToActorRefs.contains(to)) {
        val req = NetworkMessage(networkMessageID, CreditRequest(actorId))
        messageIDToIdentity(networkMessageID) = to
        idToActorRefs(to) ! req
        networkMessageID += 1
      }
  }

  override def receive: Receive = {
    sendMessagesAndReceiveAcks orElse findActorRefFromVirtualIdentity orElse {
      case other => //skip
    }
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
      try {
        parentRef ! GetActorRef(actorID, Set(self))
        queriedActorVirtualIdentities.add(actorID)
      } catch {
        case e: Throwable =>
          logger.error("Failed to fetch actorRef for " + actorID + " parentRef = " + parentRef)
      }
    }
  }
}
