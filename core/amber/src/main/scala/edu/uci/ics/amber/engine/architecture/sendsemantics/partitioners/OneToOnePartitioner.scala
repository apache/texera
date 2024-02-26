package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

case class OneToOnePartitioner(partitioning: OneToOnePartitioning) extends Partitioner {
  assert(partitioning.receivers.length == 1)

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = Iterator(0)

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers
}
