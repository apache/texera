package edu.uci.ics.amber.engine.architecture.common

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  GetActorRef,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.common.{AmberLogging, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}

import scala.collection.mutable

class AkkaActorRefMappingService(actorService: AkkaActorService) extends AmberLogging {

  override def actorId: ActorVirtualIdentity = actorService.id

  implicit val self: ActorRef = actorService.self

  private val actorRefMapping: mutable.HashMap[ActorVirtualIdentity, ActorRef] = mutable.HashMap()
  private val queriedActorVirtualIdentities = new mutable.HashSet[ActorVirtualIdentity]()
  private val toNotifyOnRegistration =
    new mutable.HashMap[ActorVirtualIdentity, mutable.Set[ActorRef]]()
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
      retrieveActorRef(id, Set())
    }
  }

  def removeActorRef(id: ActorVirtualIdentity): Unit = {
    if (actorRefMapping.contains(id)) {
      val ref = actorRefMapping.remove(id).get
      logger.warn(s"actor $id is not reachable anymore, it might have crashed. old ref = $ref")
    }
  }

  def registerActorRef(id: ActorVirtualIdentity, ref: ActorRef): Unit = {
    if (!actorRefMapping.contains(id)) {
      logger.info(s"register ${VirtualIdentityUtils.toShorterString(id)} -> $ref")
      actorRefMapping(id) = ref
      if (messageStash.contains(id)) {
        val stash = messageStash(id)
        while (stash.nonEmpty) {
          ref ! stash.dequeue()
        }
      }
    }
    if (toNotifyOnRegistration.contains(id)) {
      toNotifyOnRegistration(id).foreach { toNotify =>
        toNotify ! RegisterActorRef(id, ref)
      }
      toNotifyOnRegistration.remove(id)
    }
  }

  def retrieveActorRef(id: ActorVirtualIdentity, replyTo: Set[ActorRef]): Unit = {
    if (actorRefMapping.contains(id)) {
      replyTo.foreach { actor =>
        actor ! RegisterActorRef(id, actorRefMapping(id))
      }
    } else if (actorId != CONTROLLER) {
      // propagation stops at controller
      if (!queriedActorVirtualIdentities.contains(id)) {
        try {
          actorService.parent ! GetActorRef(id, replyTo + actorService.self)
          queriedActorVirtualIdentities.add(id)
        } catch {
          case e: Throwable =>
            logger.warn(
              s"Failed to fetch actorRef for ${VirtualIdentityUtils.toShorterString(id)} parentRef = " + actorService.parent
            )
        }
      }
    } else {
      // on controller, wait for actor ref registration.
      logger.warn(s"unknown identifier: ${VirtualIdentityUtils.toShorterString(id)}")
      val toNotifySet = toNotifyOnRegistration.getOrElseUpdate(id, mutable.HashSet[ActorRef]())
      replyTo.foreach(toNotifySet.add)
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

}
