package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

/**
  * Sending policy used by a worker to send data to the downstream workers.
  */
sealed trait DataSendingPolicy {
  val policyTag: LinkIdentity
  val batchSize: Int
  val receivers: Array[ActorVirtualIdentity]
}

case class HashBasedShufflePolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity],
    hashFunc: ITuple => Int
) extends DataSendingPolicy {}

case class OneToOnePolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy {}

case class RoundRobinPolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy {}

case class ParallelBatchingPolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy {}
