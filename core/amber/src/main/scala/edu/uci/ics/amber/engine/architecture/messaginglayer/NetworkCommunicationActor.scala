package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  GetActorRef,
  MessageBecomesDeadLetter,
  NetworkAck,
  NetworkMessage,
  RegisterActorRef,
  ResendMessages,
  SendRequest
}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.collection.mutable
import scala.concurrent.duration._

object NetworkCommunicationActor {

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
    * @param messageID
    * @param internalMessage
    */
  final case class NetworkMessage(messageID: Long, internalMessage: WorkflowMessage)

  /** Ack for NetworkMessage
    * note that it should NEVER be handled by the main thread
    * @param messageID
    */
  final case class NetworkAck(messageID: Long)

  final case class ResendMessages()

  final case class MessageBecomesDeadLetter(message: NetworkMessage)

  def props(parentSender: ActorRef): Props =
    Props(new NetworkCommunicationActor(parentSender))
}

/** This actor handles the transformation from identifier to actorRef
  * and also sends message to other actors. This is the most outer part of
  * the messaging layer.
  */
class NetworkCommunicationActor(parentRef: ActorRef) extends Actor with LazyLogging {

  val idToActorRefs = new mutable.HashMap[ActorVirtualIdentity, ActorRef]()
  val idToCongestionControls = new mutable.HashMap[ActorVirtualIdentity, CongestionControl]()
  val queriedActorVirtualIdentities = new mutable.HashSet[ActorVirtualIdentity]()
  val messageStash = new mutable.HashMap[ActorVirtualIdentity, mutable.Queue[WorkflowMessage]]

  /** keeps track of every outgoing message.
    * Each message is identified by this monotonic increasing ID.
    * It's different from the sequence number and it will only
    * be used by the output gate.
    */
  var networkMessageID = 0L
  val messageIDToIdentity = new mutable.LongMap[ActorVirtualIdentity]

  //add parent actor into idMap
  idToActorRefs(ActorVirtualIdentity.Self) = context.parent

  //register timer for resending messages
  val resendHandle: Cancellable = context.system.scheduler.schedule(
    30.seconds,
    30.seconds,
    self,
    ResendMessages
  )(context.dispatcher)

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
        throw new WorkflowRuntimeException(
          WorkflowRuntimeError(
            s"unknown identifier: $actorID",
            actorID.toString,
            Map.empty
          )
        )
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
    * @param actorID
    * @param ref
    */
  def registerActorRef(actorID: ActorVirtualIdentity, ref: ActorRef): Unit = {
    idToActorRefs(actorID) = ref
    if (messageStash.contains(actorID)) {
      val stash = messageStash(actorID)
      while (stash.nonEmpty) {
        forwardMessage(actorID, stash.dequeue())
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
        getActorRefMappingFromParent(id)
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
          ctrl.getTimedOutInTransitMessages.foreach { msg =>
            sendOrGetActorRef(actorID, msg)
          }
      }
    case MessageBecomesDeadLetter(msg) =>
      // only remove the mapping from id to actorRef
      // to trigger discover mechanism
      val actorID = messageIDToIdentity(msg.messageID)
      logger.warn(s"actor for $actorID might have crashed or failed")
      idToActorRefs.remove(actorID)
      if (parentRef != null) {
        getActorRefMappingFromParent(actorID)
      }
  }

  @inline
  private[this] def sendOrGetActorRef(actorID: ActorVirtualIdentity, msg: NetworkMessage): Unit = {
    if (idToActorRefs.contains(actorID)) {
      // if actorRef is found, directly send it
      idToActorRefs(actorID) ! msg
    } else {
      // otherwise, we ask the parent for the actorRef.
      if (parentRef != null) {
        getActorRefMappingFromParent(actorID)
      }
    }
  }

  @inline
  private[this] def getActorRefMappingFromParent(actorID: ActorVirtualIdentity): Unit = {
    if (!queriedActorVirtualIdentities.contains(actorID)) {
      parentRef ! GetActorRef(actorID, Set(self))
      queriedActorVirtualIdentities.add(actorID)
    }
  }

  override def receive: Receive = {
    sendMessagesAndReceiveAcks orElse findActorRefFromVirtualIdentity
  }

  override def postStop(): Unit = {
    resendHandle.cancel()
    logger.info(s"network communication actor for ${context.parent} stopped!")
  }
}
