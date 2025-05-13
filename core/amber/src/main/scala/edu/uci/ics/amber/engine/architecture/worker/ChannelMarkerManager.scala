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

import edu.uci.ics.amber.engine.architecture.messaginglayer.InputGateway
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerPayload
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerType.{
  NO_ALIGNMENT,
  REQUIRE_ALIGNMENT
}
import edu.uci.ics.amber.engine.common.{AmberLogging, CheckpointState}
import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ChannelMarkerIdentity
}

import scala.collection.mutable

class ChannelMarkerManager(val actorId: ActorVirtualIdentity, inputGateway: InputGateway)
    extends AmberLogging {

  private val markerReceived =
    new mutable.HashMap[ChannelMarkerIdentity, Set[ChannelIdentity]]()

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
    if (!markerReceived.contains(markerId)) {
      markerReceived(markerId) = Set()
    }
    markerReceived.update(markerId, markerReceived(markerId) + from)
    // check if the epoch marker is completed
    val markerReceivedFromAllChannels =
      getChannelsWithinScope(marker).subsetOf(markerReceived(markerId))
    val epochMarkerCompleted = marker.markerType match {
      case REQUIRE_ALIGNMENT => markerReceivedFromAllChannels
      case NO_ALIGNMENT      => markerReceived(markerId).size == 1 // only the first marker triggers
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported marker type: ${marker.markerType}"
        )
    }
    if (markerReceivedFromAllChannels) {
      markerReceived.remove(markerId) // clean up if all markers are received
    }
    epochMarkerCompleted
  }

  private def getChannelsWithinScope(marker: ChannelMarkerPayload): Set[ChannelIdentity] = {
    val upstreams = marker.scope.filter(_.toWorkerId == actorId)
    inputGateway.getAllChannels
      .map(_.channelId)
      .filter { id =>
        upstreams.contains(id)
      }
      .toSet
  }

}
