package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor._
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage
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
    * @param messageId Long, id for a NetworkMessage, used for FIFO and ExactlyOnce
    * @param internalMessage WorkflowMessage, the message payload
    */
  final case class NetworkMessage(messageId: Long, internalMessage: WorkflowMessage)

  /** Ack for NetworkMessage
    * note that it should NEVER be handled by the main thread
    *
    * @param messageId Long, id for a NetworkMessage, used for FIFO and ExactlyOnce
    */
  final case class NetworkAck(messageId: Long)

  final case class ResendMessages()

  final case class MessageBecomesDeadLetter(message: NetworkMessage)
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
  //register timer for resending messages
  val resendHandle: Cancellable = context.system.scheduler.schedule(
    30.seconds,
    30.seconds,
    self,
    ResendMessages
  )(context.dispatcher)

  //add parent actor into idMap
  idToActorRefs(SELF) = context.parent

  /** keeps track of every outgoing message.
    * Each message is identified by this monotonic increasing ID.
    * It's different from the sequence number and it will only
    * be used by the output gate.
    */
  var networkMessageID = 0L

  /** This method should always be a part of the unified WorkflowActor receiving logic.
    * 1. when an actor wants to know the actorRef of an Identifier, it replies if the mapping
    *    is known, else it will ask its parent actor.
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
    * @param actorId ActorVirtualIdentity, virtual ID of the Actor.
    * @param ref ActorRef, the actual reference of the Actor.
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

  def sendMessagesAndReceiveAcks: Receive = {
    case SendRequest(id, msg) =>
      if (idToActorRefs.contains(id)) {
        forwardMessage(id, msg)
      } else {
        val stash = messageStash.getOrElseUpdate(id, new mutable.Queue[WorkflowMessage]())
        stash.enqueue(msg)
        fetchActorRefMappingFromParent(id)
      }
    case NetworkAck(id) =>
      val actorID = messageIDToIdentity(id)
      val congestionControl = idToCongestionControls(actorID)
      congestionControl.ack(id)
      congestionControl.getBufferedMessagesToSend.foreach { msg =>
        congestionControl.markMessageInTransit(msg)
        sendOrGetActorRef(actorID, msg)
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
