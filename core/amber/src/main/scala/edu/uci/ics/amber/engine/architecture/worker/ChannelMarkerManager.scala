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
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerPayload
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerType.{NO_ALIGNMENT, PORT_ALIGNMENT, REQUIRE_ALIGNMENT}
import edu.uci.ics.amber.engine.common.{AmberLogging, CheckpointState}
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity, ChannelMarkerIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity

import scala.collection.mutable

class ChannelMarkerManager(val actorId: ActorVirtualIdentity, inputGateway: InputGateway, inputManager: InputManager)
    extends AmberLogging {

  private val markerReceived = new mutable.HashMap[ChannelMarkerIdentity, mutable.HashMap[PortIdentity, Set[ChannelIdentity]]]()

  val checkpoints = new mutable.HashMap[ChannelMarkerIdentity, CheckpointState]()

  /**
    * Determines if an epoch marker is fully received from all relevant senders within its scope.
    * This method checks if the epoch marker, based on its type, has been received from all necessary channels.
    * For markers requiring alignment, it verifies receipt from all senders in the scope. For non-aligned markers,
    * it checks if it's the first received marker. Post verification, it cleans up the markers.
    *
    * @return Boolean indicating if the epoch marker is completely received from all senders
    *         within the scope. Returns true if the marker is aligned, otherwise false.
    */
  def isMarkerAligned(
      from: ChannelIdentity,
      marker: ChannelMarkerPayload
  ): Boolean = {
    val markerId = marker.id
    val portId = inputGateway.getChannel(from).getPortId
    if (!markerReceived.contains(markerId)) {
      markerReceived(markerId) = new mutable.HashMap[PortIdentity, Set[ChannelIdentity]]()
    }
    val portMap = markerReceived(markerId)
    if (!portMap.contains(portId)) {
      portMap(portId) = Set()
    }
    portMap.update(portId, portMap(portId) + from)
    // check if the epoch marker is completed

    val epochMarkerCompleted = marker.markerType match {
      case REQUIRE_ALIGNMENT =>
        val markerReceivedFromAllChannels =
          getChannelsWithinScope(marker).subsetOf(portMap.values.flatten.toSet)
        if (markerReceivedFromAllChannels) {
          markerReceived.remove(markerId) // clean up if all markers are received
        }
        markerReceivedFromAllChannels
      case PORT_ALIGNMENT    =>
        val markerReceivedFromCurrentPort =
          getChannelsWithinPort(marker, portId).subsetOf(portMap(portId))
        if (markerReceivedFromCurrentPort) {
          portMap.remove(portId)
        }
        markerReceivedFromCurrentPort
      case NO_ALIGNMENT      =>
        markerReceived(markerId).size == 1 // only the first marker triggers
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported marker type: ${marker.markerType}"
        )
    }
    epochMarkerCompleted
  }

  private def filterChannels(channels: Iterable[ChannelIdentity], marker: ChannelMarkerPayload): Set[ChannelIdentity] = {
    if (marker.scope.isEmpty) channels.toSet
    else {
      val upstreams = marker.scope.filter(_.toWorkerId == actorId).toSet
      channels.filter(upstreams.contains).toSet
    }
  }

  private def getChannelsWithinScope(marker: ChannelMarkerPayload): Set[ChannelIdentity] = {
    filterChannels(inputGateway.getAllDataChannels.map(_.channelId), marker)
  }

  private def getChannelsWithinPort(marker: ChannelMarkerPayload, portId: PortIdentity): Set[ChannelIdentity] = {
    filterChannels(inputManager.getPort(portId).channels, marker)
  }
}
