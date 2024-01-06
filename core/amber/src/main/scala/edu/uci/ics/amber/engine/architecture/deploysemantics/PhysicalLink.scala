package edu.uci.ics.amber.engine.architecture.deploysemantics

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  HashBasedShufflePartitioning,
  OneToOnePartitioning,
  Partitioning,
  RangeBasedShufflePartitioning,
  RoundRobinPartitioning
}
import edu.uci.ics.amber.engine.common.AmberConfig.defaultBatchSize
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, PhysicalLinkIdentity}
import edu.uci.ics.texera.workflow.common.workflow.{
  BroadcastPartition,
  HashPartition,
  PartitionInfo,
  RangePartition,
  SinglePartition,
  UnknownPartition
}

object PhysicalLink {
  def apply(
      fromPhysicalOp: PhysicalOp,
      fromPort: Int,
      toPhysicalOp: PhysicalOp,
      inputPort: Int
  ): PhysicalLink = {
    new PhysicalLink(
      fromPhysicalOp,
      fromPort,
      toPhysicalOp,
      inputPort,
      List()
    )
  }
  def apply(
      fromPhysicalOp: PhysicalOp,
      fromPort: Int,
      toPhysicalOp: PhysicalOp,
      inputPort: Int,
      partitionInfo: PartitionInfo
  ): PhysicalLink = {
    val partitionings: List[(Partitioning, List[ActorVirtualIdentity])] = partitionInfo match {
      case HashPartition(hashColumnIndices) =>
        fromPhysicalOp.getWorkerIds
          .map(_ =>
            (
              HashBasedShufflePartitioning(
                defaultBatchSize,
                toPhysicalOp.getWorkerIds,
                hashColumnIndices
              ),
              toPhysicalOp.getWorkerIds
            )
          )

      case RangePartition(rangeColumnIndices, rangeMin, rangeMax) =>
        fromPhysicalOp.getWorkerIds
          .map(_ =>
            (
              RangeBasedShufflePartitioning(
                defaultBatchSize,
                toPhysicalOp.getWorkerIds,
                rangeColumnIndices,
                rangeMin,
                rangeMax
              ),
              toPhysicalOp.getWorkerIds
            )
          )

      case SinglePartition() =>
        assert(!toPhysicalOp.parallelizable)
        fromPhysicalOp.getWorkerIds
          .map(_ =>
            (
              OneToOnePartitioning(defaultBatchSize, Array(toPhysicalOp.getWorkerIds.head)),
              toPhysicalOp.getWorkerIds
            )
          )

      case BroadcastPartition() =>
        fromPhysicalOp.getWorkerIds
          .map(_ =>
            (
              BroadcastPartitioning(defaultBatchSize, toPhysicalOp.getWorkerIds),
              toPhysicalOp.getWorkerIds
            )
          )

      case UnknownPartition() =>
        fromPhysicalOp.getWorkerIds
          .map(_ =>
            (
              RoundRobinPartitioning(defaultBatchSize, toPhysicalOp.getWorkerIds),
              toPhysicalOp.getWorkerIds
            )
          )

      case _ =>
        List()

    }

    new PhysicalLink(
      fromPhysicalOp,
      fromPort,
      toPhysicalOp,
      inputPort,
      partitionings
    )
  }
}
class PhysicalLink(
    @transient
    val fromOp: PhysicalOp,
    val fromPort: Int,
    @transient
    val toOp: PhysicalOp,
    val toPort: Int,
    val partitionings: List[(Partitioning, List[ActorVirtualIdentity])]
) extends Serializable {

  val id: PhysicalLinkIdentity = PhysicalLinkIdentity(fromOp.id, fromPort, toOp.id, toPort)
  def totalReceiversCount: Long = partitionings.length

}
