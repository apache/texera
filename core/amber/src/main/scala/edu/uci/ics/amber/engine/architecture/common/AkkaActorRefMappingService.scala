/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.engine.architecture.common

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.{
  CreditRequest,
  GetActorRef,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.util.VirtualIdentityUtils
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

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

  def askForCredit(channelId: ChannelIdentity): Unit = {
    val id = channelId.toWorkerId
    if (actorRefMapping.contains(id)) {
      actorRefMapping(id) ! CreditRequest(channelId)
    }
  }

  def hasActorRef(id: ActorVirtualIdentity): Boolean = {
    actorRefMapping.contains(id)
  }

  def forwardToActor(msg: NetworkMessage): Unit = {
    val id = msg.internalMessage.channelId.toWorkerId
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
