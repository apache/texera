package edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

/**
  * Partitioning used by a worker to send data to the downstream workers.
  */
sealed trait Partitioning {
  val batchSize: Int
  val receivers: Array[ActorVirtualIdentity]
}

case class HashBasedShufflePartitioning(
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity],
    hashColumnIndices: Array[Int]
) extends Partitioning {}

case class OneToOnePartitioning(
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends Partitioning {}

case class RoundRobinPartitioning(
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends Partitioning {}
