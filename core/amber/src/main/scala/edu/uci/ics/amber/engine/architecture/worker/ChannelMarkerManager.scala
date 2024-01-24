package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.messaginglayer.InputGateway
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelMarkerPayload,
  NoAlignment,
  RequireAlignment
}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ChannelMarkerIdentity
}

import scala.collection.mutable

class ChannelMarkerManager(val actorId: ActorVirtualIdentity, inputGateway: InputGateway)
    extends AmberLogging {

  private val markerReceived =
    new mutable.HashMap[ChannelMarkerIdentity, Set[ChannelIdentity]]().withDefaultValue(Set())

  /**
    * Checks if a channel marker is aligned, indicating if an epoch marker has been completely received
    * from all relevant senders within its scope.
    *
    * This method marks the arrival of a channel marker and determines if the epoch marker
    * is completely received. The check is based on the upstream link status and the received marker.
    * If the marker type requires alignment, it verifies that markers have been received from all
    * senders within the scope. For markers that do not require alignment, it checks if the marker is
    * the first one received. The method also handles the cleanup of received markers once they are
    * aligned.
    *
    * @param upstreamLinkStatus The current status of upstream links, indicating which senders
    *                           have not yet completed sending markers.
    * @param from               The channel ID from which the current marker has arrived.
    * @param marker             The payload of the channel marker being checked for alignment.
    * @return Boolean indicating if the epoch marker is completely received from all senders
    *         within the scope. Returns true if the marker is aligned, otherwise false.
    */
  def isMarkerAligned(
      upstreamLinkStatus: UpstreamLinkStatus,
      from: ChannelIdentity,
      marker: ChannelMarkerPayload
  ): Boolean = {
    val markerId = marker.id
    markerReceived.update(markerId, markerReceived(markerId) + from)
    // check if the epoch marker is completed
    val upstreams = marker.scope.filter(_.toWorkerId == actorId)
    val sendersWithinScope = inputGateway.getAllChannels
      .map(_.channelId)
      .filter { id =>
        upstreams.contains(id)
      }
      .toSet
    val markerReceivedFromAllChannels = sendersWithinScope.subsetOf(markerReceived(markerId))
    val markerCompleted = marker.markerType match {
      case RequireAlignment => markerReceivedFromAllChannels
      case NoAlignment      => markerReceived(markerId).size == 1 // only the first marker triggers
    }
    if (markerReceivedFromAllChannels) {
      markerReceived.remove(markerId) // clean up if all markers are received
    }
    markerCompleted
  }

}
