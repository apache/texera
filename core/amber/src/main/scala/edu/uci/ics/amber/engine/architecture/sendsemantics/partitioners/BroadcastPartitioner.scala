package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.BroadcastPartitioning
import edu.uci.ics.amber.engine.common.model.tuple.Tuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class BroadcastPartitioner(partitioning: BroadcastPartitioning) extends Partitioner {

  private val receivers = partitioning.channels.map(_.toWorkerId).distinct

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    receivers.indices.iterator
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = receivers
}
