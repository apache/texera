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
        fromPhysicalOp.getIdentifiers
          .map(_ =>
            (
              HashBasedShufflePartitioning(
                defaultBatchSize,
                toPhysicalOp.getIdentifiers,
                hashColumnIndices
              ),
              toPhysicalOp.getIdentifiers
            )
          )

      case RangePartition(rangeColumnIndices, rangeMin, rangeMax) =>
        fromPhysicalOp.getIdentifiers
          .map(_ =>
            (
              RangeBasedShufflePartitioning(
                defaultBatchSize,
                toPhysicalOp.getIdentifiers,
                rangeColumnIndices,
                rangeMin,
                rangeMax
              ),
              toPhysicalOp.getIdentifiers
            )
          )

      case SinglePartition() =>
        assert(toPhysicalOp.parallelizable == false)
        fromPhysicalOp.getIdentifiers
          .map(_ =>
            (
              OneToOnePartitioning(defaultBatchSize, Array(toPhysicalOp.getIdentifiers.head)),
              toPhysicalOp.getIdentifiers
            )
          )

      case BroadcastPartition() =>
        fromPhysicalOp.getIdentifiers
          .map(_ =>
            (
              BroadcastPartitioning(defaultBatchSize, toPhysicalOp.getIdentifiers),
              toPhysicalOp.getIdentifiers
            )
          )

      case UnknownPartition() =>
        fromPhysicalOp.getIdentifiers
          .map(_ =>
            (
              RoundRobinPartitioning(defaultBatchSize, toPhysicalOp.getIdentifiers),
              toPhysicalOp.getIdentifiers
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
  def totalReceiversCount: Long = toOp.getIdentifiers.length

}
