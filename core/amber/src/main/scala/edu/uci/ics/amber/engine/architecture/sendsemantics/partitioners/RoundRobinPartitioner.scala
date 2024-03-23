package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RoundRobinPartitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

case class RoundRobinPartitioner(partitioning: RoundRobinPartitioning) extends Partitioner {
  var roundRobinIndex = 0

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    roundRobinIndex = (roundRobinIndex + 1) % partitioning.receivers.length
    Iterator(roundRobinIndex)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers
}
