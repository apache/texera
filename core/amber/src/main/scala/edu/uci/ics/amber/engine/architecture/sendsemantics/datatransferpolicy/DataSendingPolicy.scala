package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

/**
  * Sending policy used by a worker to send data to the downstream workers.
  */
sealed trait DataSendingPolicy {
  val batchSize: Int
  val receivers: Array[ActorVirtualIdentity]
}

case class HashBasedShufflePolicy(
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity],
    hashFunc: ITuple => Int
) extends DataSendingPolicy {}

case class OneToOnePolicy(
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy {}

case class RoundRobinPolicy(
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy {}

case class ParallelBatchingPolicy(
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy {}
