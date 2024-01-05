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
      Array()
    )
  }
  def apply(
      fromPhysicalOp: PhysicalOp,
      fromPort: Int,
      toPhysicalOp: PhysicalOp,
      inputPort: Int,
      partitionInfo: PartitionInfo
  ): PhysicalLink = {
    val partitionings: Array[(Partitioning, Array[ActorVirtualIdentity])] = partitionInfo match {
      case HashPartition(hashColumnIndices) =>
        fromPhysicalOp.identifiers
          .map(_ =>
            (
              HashBasedShufflePartitioning(
                defaultBatchSize,
                toPhysicalOp.identifiers,
                hashColumnIndices
              ),
              toPhysicalOp.identifiers
            )
          )

      case RangePartition(rangeColumnIndices, rangeMin, rangeMax) =>
        fromPhysicalOp.identifiers
          .map(_ =>
            (
              RangeBasedShufflePartitioning(
                defaultBatchSize,
                toPhysicalOp.identifiers,
                rangeColumnIndices,
                rangeMin,
                rangeMax
              ),
              toPhysicalOp.identifiers
            )
          )

      case SinglePartition() =>
        assert(toPhysicalOp.numWorkers == 1)
        fromPhysicalOp.identifiers
          .map(_ =>
            (
              OneToOnePartitioning(defaultBatchSize, Array(toPhysicalOp.identifiers.head)),
              toPhysicalOp.identifiers
            )
          )

      case BroadcastPartition() =>
        fromPhysicalOp.identifiers
          .map(_ =>
            (
              BroadcastPartitioning(defaultBatchSize, toPhysicalOp.identifiers),
              toPhysicalOp.identifiers
            )
          )

      case UnknownPartition() =>
        fromPhysicalOp.identifiers
          .map(_ =>
            (
              RoundRobinPartitioning(defaultBatchSize, toPhysicalOp.identifiers),
              toPhysicalOp.identifiers
            )
          )

      case _ =>
        Array()

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
    val partitionings: Array[(Partitioning, Array[ActorVirtualIdentity])]
) extends Serializable {

  val id: PhysicalLinkIdentity = PhysicalLinkIdentity(fromOp.id, fromPort, toOp.id, toPort)
  def totalReceiversCount: Long = toOp.numWorkers

}
