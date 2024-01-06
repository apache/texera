package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class ChannelConfig(
    partitioning: (Partitioning, List[ActorVirtualIdentity])
)
