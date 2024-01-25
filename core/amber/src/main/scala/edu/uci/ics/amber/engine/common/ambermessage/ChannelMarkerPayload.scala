package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ChannelMarkerIdentity
}

sealed trait ChannelMarkerType
case object RequireAlignment extends ChannelMarkerType
case object NoAlignment extends ChannelMarkerType

final case class ChannelMarkerPayload(
    id: ChannelMarkerIdentity,
    markerType: ChannelMarkerType,
    scope: Set[ChannelIdentity],
    commandMapping: Map[ActorVirtualIdentity, ControlInvocation]
) extends WorkflowFIFOMessagePayload
