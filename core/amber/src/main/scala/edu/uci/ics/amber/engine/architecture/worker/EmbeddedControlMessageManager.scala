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

package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.messaginglayer.{InputGateway, InputManager}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessage
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.{
  NO_ALIGNMENT,
  PORT_ALIGNMENT,
  ALL_ALIGNMENT
}
import edu.uci.ics.amber.engine.common.{AmberLogging, CheckpointState}
import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}

import scala.collection.mutable

class EmbeddedControlMessageManager(
    val actorId: ActorVirtualIdentity,
    inputGateway: InputGateway,
    inputManager: InputManager
) extends AmberLogging {

  private val ecmReceived =
    new mutable.HashMap[EmbeddedControlMessageIdentity, Set[ChannelIdentity]]()

  val checkpoints = new mutable.HashMap[EmbeddedControlMessageIdentity, CheckpointState]()

  /**
    * Determines if an ECM is fully received from all relevant senders within its scope.
    * This method checks if the ECM, based on its type, has been received from all necessary channels.
    * For ECMs requiring alignment, it verifies receipt from all senders in the scope. For non-aligned ECMs,
    * it checks if it's the first received ECM. Post verification, it cleans up the ECMs.
    *
    * @return Boolean indicating if the ECM is completely received from all senders
    *         within the scope. Returns true if the ECM is aligned, otherwise false.
    */
  def isECMAligned(
      from: ChannelIdentity,
      ecm: EmbeddedControlMessage
  ): Boolean = {
    val portId = inputGateway.getChannel(from).getPortId
    if (!ecmReceived.contains(ecm.id)) {
      ecmReceived(ecm.id) = Set()
    }
    ecmReceived.update(ecm.id, ecmReceived(ecm.id) + from)
    val ecmReceivedFromAllChannels =
      getChannelsWithinScope(ecm).subsetOf(ecmReceived(ecm.id))
    // check if the ECM is completed
    val ecmCompleted = ecm.ecmType match {
      case ALL_ALIGNMENT =>
        ecmReceivedFromAllChannels
      case PORT_ALIGNMENT =>
        inputManager.getPort(portId).channels.subsetOf(ecmReceived(ecm.id))
      case NO_ALIGNMENT =>
        ecmReceived(ecm.id).size == 1 // only the first ECM triggers
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported ECM type: ${ecm.ecmType}"
        )
    }
    if (ecmReceivedFromAllChannels) {
      ecmReceived.remove(ecm.id) // clean up if all ECMs are received
    }
    ecmCompleted
  }

  private def getChannelsWithinScope(ecm: EmbeddedControlMessage): Set[ChannelIdentity] = {
    if (ecm.scope.isEmpty) inputGateway.getAllDataChannels.map(_.channelId)
    else {
      val upstreams = ecm.scope.filter(_.toWorkerId == actorId)
      inputGateway.getAllChannels
        .map(_.channelId)
        .filter { id =>
          upstreams.contains(id)
        }
    }
  }.toSet
}
