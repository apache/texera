package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.model.tuple.Tuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class HashBasedShufflePartitioner(partitioning: HashBasedShufflePartitioning)
    extends Partitioner {

  private val receivers = partitioning.channels.map(_.toWorkerId).distinct

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    val numBuckets = receivers.length
    val partialTuple =
      if (partitioning.hashAttributeNames.isEmpty) tuple
      else tuple.getPartialTuple(partitioning.hashAttributeNames.toList)
    val index = Math.floorMod(partialTuple.hashCode(), numBuckets)
    Iterator(index)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = receivers
}
