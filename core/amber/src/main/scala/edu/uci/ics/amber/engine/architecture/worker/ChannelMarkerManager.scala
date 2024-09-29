package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.messaginglayer.InputGateway
import edu.uci.ics.amber.engine.common.{AmberLogging, CheckpointState}
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

object ChannelMarkerManager {
  final case class MarkerContext(marker: ChannelMarkerPayload, fromChannel: ChannelIdentity)
}

class ChannelMarkerManager(val actorId: ActorVirtualIdentity, inputGateway: InputGateway)
    extends AmberLogging {

  private val markerReceived =
    new mutable.HashMap[ChannelMarkerIdentity, Set[ChannelIdentity]]()

  @transient
  lazy val checkpoints = new mutable.HashMap[ChannelMarkerIdentity, CheckpointState]()

  /**
    * Determines if an epoch marker is fully received from all relevant senders within its scope.
    * This method checks if the epoch marker, based on its type, has been received from all necessary channels.
    * For markers requiring alignment, it verifies receipt from all senders in the scope. For non-aligned markers,
    * it checks if it's the first received marker. Post verification, it cleans up the markers.
    *
    * @return Boolean indicating if the epoch marker is completely received from all senders
    *          within the scope. Returns true if the marker is aligned, otherwise false.
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
      case RequireAlignment => markerReceivedFromAllChannels
      case NoAlignment      => markerReceived(markerId).size == 1 // only the first marker triggers
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
