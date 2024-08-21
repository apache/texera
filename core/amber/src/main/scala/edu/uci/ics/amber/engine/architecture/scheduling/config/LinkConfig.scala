package edu.uci.ics.amber.engine.architecture.scheduling.config
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  Partitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
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
      partitionInfo: PartitionInfo,
      batchSize: Int
  ): Partitioning = {
    partitionInfo match {
      case HashPartition(hashAttributeNames) =>
        HashBasedShufflePartitioning(
          batchSize,
          fromWorkerIds.flatMap(from =>
            toWorkerIds.map(to => ChannelIdentity(from, to, isControl = false))
          ),
          hashAttributeNames
        )

      case RangePartition(rangeAttributeNames, rangeMin, rangeMax) =>
        RangeBasedShufflePartitioning(
          batchSize,
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
          batchSize,
          fromWorkerIds.map(fromWorkerId =>
            ChannelIdentity(fromWorkerId, toWorkerIds.head, isControl = false)
          )
        )

      case OneToOnePartition() =>
        OneToOnePartitioning(
          batchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case BroadcastPartition() =>
        BroadcastPartitioning(
          batchSize,
          fromWorkerIds.zip(toWorkerIds).map {
            case (fromWorkerId, toWorkerId) =>
              ChannelIdentity(fromWorkerId, toWorkerId, isControl = false)
          }
        )

      case UnknownPartition() =>
        RoundRobinPartitioning(
          batchSize,
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
