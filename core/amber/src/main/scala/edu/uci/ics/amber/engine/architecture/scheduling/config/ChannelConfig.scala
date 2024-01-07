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
      case HashPartition(hashColumnIndices) =>
        fromWorkerIds
          .map(_ =>
            ChannelConfig(
              HashBasedShufflePartitioning(
                defaultBatchSize,
                toWorkerIds,
                hashColumnIndices
              ),
              toWorkerIds
            )
          )

      case RangePartition(rangeColumnIndices, rangeMin, rangeMax) =>
        fromWorkerIds
          .map(_ =>
            ChannelConfig(
              RangeBasedShufflePartitioning(
                defaultBatchSize,
                toWorkerIds,
                rangeColumnIndices,
                rangeMin,
                rangeMax
              ),
              toWorkerIds
            )
          )

      case SinglePartition() =>
        assert(toWorkerIds.size == 1)
        fromWorkerIds
          .map(_ =>
            ChannelConfig(
              OneToOnePartitioning(defaultBatchSize, Array(toWorkerIds.head)),
              toWorkerIds
            )
          )

      case BroadcastPartition() =>
        fromWorkerIds
          .map(_ =>
            ChannelConfig(
              BroadcastPartitioning(defaultBatchSize, toWorkerIds),
              toWorkerIds
            )
          )

      case UnknownPartition() =>
        fromWorkerIds
          .map(_ =>
            ChannelConfig(
              RoundRobinPartitioning(defaultBatchSize, toWorkerIds),
              toWorkerIds
            )
          )

      case _ =>
        List()

    }
  }
}
case class ChannelConfig(
    partitioning: Partitioning,
    receiverWorkerIds: List[ActorVirtualIdentity]
)
