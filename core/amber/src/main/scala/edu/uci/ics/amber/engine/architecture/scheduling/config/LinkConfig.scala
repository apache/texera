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
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings._
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

case object LinkConfig {
  def toPartitioning(
      fromWorkerIds: List[ActorVirtualIdentity],
      toWorkerIds: List[ActorVirtualIdentity],
      partitionInfo: PartitionInfo,
      dataTransferBatchSize: Int
  ): Partitioning = {
    partitionInfo match {
      case HashPartition(hashAttributeNames) =>
        HashBasedShufflePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.flatMap(from =>
            toWorkerIds.map(to => ChannelIdentity(from, to, isControl = false))
          ),
          hashAttributeNames
        )

      case RangePartition(rangeAttributeNames, rangeMin, rangeMax) =>
        RangeBasedShufflePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.flatMap(fromId =>
            toWorkerIds.map(toId => ChannelIdentity(fromId, toId, isControl = false))
          ),
          rangeAttributeNames,
          rangeMin,
          rangeMax
        )

      case SinglePartition() =>
        assert(toWorkerIds.size == 1)
        OneToOnePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.map(fromWorkerId =>
            ChannelIdentity(fromWorkerId, toWorkerIds.head, isControl = false)
          )
        )

      case OneToOnePartition() =>
        OneToOnePartitioning(
          dataTransferBatchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case BroadcastPartition() =>
        BroadcastPartitioning(
          dataTransferBatchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case UnknownPartition() =>
        RoundRobinPartitioning(
          dataTransferBatchSize,
          fromWorkerIds.flatMap(from =>
            toWorkerIds.map(to => ChannelIdentity(from, to, isControl = false))
          )
        )

      case _ =>
        throw new UnsupportedOperationException()

    }
  }
}

case class LinkConfig(
    channelConfigs: List[ChannelConfig],
    partitioning: Partitioning
)
