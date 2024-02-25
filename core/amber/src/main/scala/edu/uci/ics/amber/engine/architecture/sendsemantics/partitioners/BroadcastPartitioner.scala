package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.BroadcastPartitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

case class BroadcastPartitioner(partitioning: BroadcastPartitioning) extends Partitioner {

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    partitioning.receivers.indices.iterator
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers
}
