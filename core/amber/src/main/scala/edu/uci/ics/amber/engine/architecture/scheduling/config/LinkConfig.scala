package edu.uci.ics.amber.engine.architecture.scheduling.config
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  Partitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
import edu.uci.ics.amber.engine.common.AmberConfig.defaultBatchSize
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.texera.workflow.common.workflow.{
  BroadcastPartition,
  HashPartition,
  OneToOnePartition,
  PartitionInfo,
  RangePartition,
  SinglePartition,
  UnknownPartition
}

case object LinkConfig {
  def toPartitioning(
      fromWorkerIds: List[ActorVirtualIdentity],
      toWorkerIds: List[ActorVirtualIdentity],
      partitionInfo: PartitionInfo
  ): Partitioning = {
    partitionInfo match {
      case HashPartition(hashAttributeNames) =>
        HashBasedShufflePartitioning(
          defaultBatchSize,
          fromWorkerIds.flatMap(from =>
            toWorkerIds.map(to => ChannelIdentity(from, to, isControl = false))
          ),
          hashAttributeNames
        )

      case RangePartition(rangeAttributeNames, rangeMin, rangeMax) =>
        RangeBasedShufflePartitioning(
          defaultBatchSize,
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
          defaultBatchSize,
          fromWorkerIds.map(fromWorkerId =>
            ChannelIdentity(fromWorkerId, toWorkerIds.head, isControl = false)
          )
        )

      case OneToOnePartition() =>
        OneToOnePartitioning(
          defaultBatchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case BroadcastPartition() =>
        BroadcastPartitioning(
          defaultBatchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case UnknownPartition() =>
        RoundRobinPartitioning(
          defaultBatchSize,
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
