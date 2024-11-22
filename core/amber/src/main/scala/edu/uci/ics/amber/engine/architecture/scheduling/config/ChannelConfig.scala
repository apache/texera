package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.core.workflow.{
  BroadcastPartition,
  HashPartition,
  OneToOnePartition,
  PartitionInfo,
  RangePartition,
  SinglePartition,
  UnknownPartition
}
import edu.uci.ics.amber.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.workflow.PortIdentity

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
      case OneToOnePartition() =>
        fromWorkerIds.zip(toWorkerIds).map {
          case (fromWorkerId, toWorkerId) =>
            ChannelConfig(ChannelIdentity(fromWorkerId, toWorkerId, isControl = false), toPortId)
        }
      case _ =>
        List()

    }
  }
}

case class ChannelConfig(
    channelId: ChannelIdentity,
    toPortId: PortIdentity
)
