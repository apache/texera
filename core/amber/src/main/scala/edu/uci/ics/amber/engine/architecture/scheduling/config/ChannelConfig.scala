package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.workflow.{
  BroadcastPartition,
  HashPartition,
  PartitionInfo,
  RangePartition,
  SinglePartition,
  UnknownPartition
}

case object ChannelConfig {
  def generateChannelConfigs(
      fromWorkerIds: List[ActorVirtualIdentity],
      toWorkerIds: List[ActorVirtualIdentity],
      toPortId: PortIdentity,
      partitionInfo: PartitionInfo
  ): List[ChannelConfig] = {
    partitionInfo match {
      case HashPartition(_) | RangePartition(_, _, _) | BroadcastPartition() | UnknownPartition() =>
        fromWorkerIds.flatMap(fromWorkerId =>
          toWorkerIds.map(toWorkerId =>
            ChannelConfig(ChannelIdentity(fromWorkerId, toWorkerId, isControl = false), toPortId)
          )
        )

      case SinglePartition() =>
        assert(toWorkerIds.size == 1)
        val toWorkerId = toWorkerIds.head
        fromWorkerIds.map(fromWorkerId =>
          ChannelConfig(ChannelIdentity(fromWorkerId, toWorkerId, isControl = false), toPortId)
        )
      case _ =>
        List()

    }
  }
}
case class ChannelConfig(
    channelId: ChannelIdentity,
    toPortId: PortIdentity
)
