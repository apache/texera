package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class HashBasedShufflePartitioner(partitioning: HashBasedShufflePartitioning)
    extends Partitioner {

  override def getBucketIndex(tuple: ITuple): Int = {
    val numBuckets = partitioning.receivers.length

    val index = (partitioning.hashColumnIndices
      .map(i => tuple.get(i))
      .toList
      .hashCode() % numBuckets + numBuckets) % numBuckets

    index
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers
}
