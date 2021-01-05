package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.{Actor, ActorRef}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGate.{
  NetworkMessage,
  GetActorRef,
  ReplyActorRef
}
import edu.uci.ics.amber.engine.common.ambermessage.neo.WorkflowMessage
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable

object NetworkOutputGate {

  /** Identifier <-> ActorRef related messages
    */
  final case class GetActorRef(id: ActorVirtualIdentity, replyTo: ActorRef)
  final case class ReplyActorRef(id: ActorVirtualIdentity, ref: ActorRef)

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
}

/** This trait handles the transformation from identifier to actorRef
  * and also sends message to other actors. This is the most outer part of
  * the messaging layer.
  * It SHOULD be a trait since it is highly coupled with actors' sending/receiving logic.
  * TODO: this trait should eventually become another actor, which is the sender actor.
  */
trait NetworkOutputGate {
  this: Actor => // it requires the class to be an actor.

  private val idToActorRefs = mutable.HashMap[ActorVirtualIdentity, ActorRef]()
  private val messageStash = mutable.HashMap[ActorVirtualIdentity, mutable.Queue[WorkflowMessage]]()

  /** keeps track of every outgoing message.
    * Each message is identified by this monotonic increasing ID.
    * It's different from the sequence number and it will only
    * be used by the output gate.
    */
  private var networkMessageID = 0L

  //add self into idMap
  idToActorRefs(VirtualIdentity.Self) = self

  /** This method should always be a part of the unified WorkflowActor receiving logic.
    * 1. when an actor wants to know the actorRef of an Identifier, it replies if the mapping
    *    is known, else it will ask its parent actor.
    * 2. when it receives a mapping, it adds that mapping to the state.
    */
  def findActorRefFromVirtualIdentity: Receive = {
    case GetActorRef(id, replyTo) =>
      if (idToActorRefs.contains(id)) {
        replyTo ! ReplyActorRef(id, idToActorRefs(id))
      } else {
        context.parent ! GetActorRef(id, replyTo)
      }
    case ReplyActorRef(id, ref) =>
      registerActorRef(id, ref)
  }

  /** This method forward a message by using tell pattern
    * if the map from Identifier to ActorRef is known,
    * forward the message immediately,
    * otherwise stash the message and ask parent for help.
    */
  def forwardMessage(to: ActorVirtualIdentity, message: WorkflowMessage): Unit = {
    if (idToActorRefs.contains(to)) {
      forward(idToActorRefs(to), message)
    } else {
      messageStash.getOrElseUpdate(to, new mutable.Queue[WorkflowMessage]()).enqueue(message)
      context.parent ! GetActorRef(to, self)
    }
  }

  /** Send message to another actor.
    * @param to
    * @param message
    */
  private def forward(to: ActorRef, message: WorkflowMessage): Unit = {
    to ! NetworkMessage(networkMessageID, message)
    networkMessageID += 1
  }

  /** Add one mapping from Identifier to ActorRef into its state.
    * If there are unsent messages for the actor, send them.
    * @param id
    * @param ref
    */
  def registerActorRef(id: ActorVirtualIdentity, ref: ActorRef): Unit = {
    idToActorRefs(id) = ref
    if (messageStash.contains(id)) {
      val stash = messageStash(id)
      while (stash.nonEmpty) {
        forward(ref, stash.dequeue())
      }
    }
  }

}
