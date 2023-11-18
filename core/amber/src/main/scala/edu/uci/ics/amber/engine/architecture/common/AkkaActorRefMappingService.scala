package edu.uci.ics.amber.engine.architecture.common

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  GetActorRef,
  NetworkMessage,
  RegisterActorRef,
  CreditRequest
}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import scala.collection.mutable

class AkkaActorRefMappingService(actorService: AkkaActorService) extends AmberLogging {

  override def actorId: ActorVirtualIdentity = actorService.id

  implicit val self: ActorRef = actorService.self

  private val actorRefMapping: mutable.HashMap[ActorVirtualIdentity, ActorRef] = mutable.HashMap()
  private val queriedActorVirtualIdentities = new mutable.HashSet[ActorVirtualIdentity]()
  private val messageStash =
    new mutable.HashMap[ActorVirtualIdentity, mutable.Queue[NetworkMessage]]
  actorRefMapping(SELF) = actorService.self

  def askForCredit(channel: ChannelID): Unit = {
    val id = channel.to
    if (actorRefMapping.contains(id)) {
      actorRefMapping(id) ! CreditRequest(channel)
    }
  }

  def hasActorRef(id: ActorVirtualIdentity): Boolean = {
    actorRefMapping.contains(id)
  }

  def forwardToActor(msg: NetworkMessage): Unit = {
    val id = msg.internalMessage.channel.to
    if (actorRefMapping.contains(id)) {
      actorRefMapping(id) ! msg
    } else {
      val stash = messageStash.getOrElseUpdate(id, new mutable.Queue[NetworkMessage]())
      stash.enqueue(msg)
      fetchActorRefMappingFromParent(id)
    }
  }

  def removeActorRef(id: ActorVirtualIdentity): Unit = {
    if (actorRefMapping.contains(id)) {
      val ref = actorRefMapping.remove(id).get
      logger.error(s"actor $id is not reachable anymore, it might have crashed. old ref = $ref")
    }
  }

  def registerActorRef(id: ActorVirtualIdentity, ref: ActorRef): Unit = {
    if (!actorRefMapping.contains(id)) {
      logger.info(s"register $id -> $ref")
      actorRefMapping(id) = ref
      if (messageStash.contains(id)) {
        val stash = messageStash(id)
        while (stash.nonEmpty) {
          ref ! stash.dequeue()
        }
      }
    }
  }

  def retrieveActorRef(id: ActorVirtualIdentity, replyTo: Set[ActorRef]): Unit = {
    if (actorRefMapping.contains(id)) {
      replyTo.foreach { actor =>
        actor ! RegisterActorRef(id, actorRefMapping(id))
      }
    } else if (actorService.parent != null) {
      actorService.parent ! GetActorRef(id, replyTo + actorService.self)
    } else {
      logger.error(s"unknown identifier: $id")
    }
  }

  def clearQueriedActorRefs(): Unit = {
    queriedActorVirtualIdentities.clear()
  }

  def findActorVirtualIdentity(ref: ActorRef): Option[ActorVirtualIdentity] = {
    actorRefMapping
      .find {
        case (_, actorRef) =>
          actorRef == ref
      }
      .map(_._1)
  }

  @inline
  private[this] def fetchActorRefMappingFromParent(id: ActorVirtualIdentity): Unit = {
    if (!queriedActorVirtualIdentities.contains(id)) {
      try {
        actorService.parent ! GetActorRef(id, Set(actorService.self))
        queriedActorVirtualIdentities.add(id)
      } catch {
        case e: Throwable =>
          logger.error("Failed to fetch actorRef for " + id + " parentRef = " + actorService.parent)
      }
    }
  }
}
