package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerPayload

trait InternalMarker extends TupleLike {
  override def getFields: Array[Any] = Array.empty
}

final case class FinalizePort(portId: PortIdentity, input: Boolean) extends InternalMarker
final case class FinalizeExecutor(marker: ChannelMarkerPayload) extends InternalMarker
