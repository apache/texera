package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
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
      partitionInfo: PartitionInfo
  ): List[ChannelConfig] = {
    partitionInfo match {
      case HashPartition(_) | RangePartition(_, _, _) | BroadcastPartition() | UnknownPartition() =>
        fromWorkerIds.flatMap(fromWorkerId =>
          toWorkerIds.map(toWorkerId => ChannelConfig(fromWorkerId, toWorkerId))
        )

      case SinglePartition() =>
        assert(toWorkerIds.size == 1)
        val toWorkerId = toWorkerIds.head
        fromWorkerIds.map(fromWorkerId => ChannelConfig(fromWorkerId, toWorkerId))
      case _ =>
        List()

    }
  }
}
case class ChannelConfig(
    fromWorkerId: ActorVirtualIdentity,
    toWorkerId: ActorVirtualIdentity
)
