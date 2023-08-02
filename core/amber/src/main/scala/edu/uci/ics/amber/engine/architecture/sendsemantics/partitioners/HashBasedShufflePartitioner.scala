package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

case class HashBasedShufflePartitioner(partitioning: HashBasedShufflePartitioning)
    extends Partitioner {

  override def getBucketIndex(tuple: ITuple): Iterator[Int] = {
    val numBuckets = partitioning.receivers.length
    val index = Math.floorMod(
      tuple
        .asInstanceOf[Tuple]
        .getPartialTuple(partitioning.hashColumnIndices.toArray)
        .hashCode(),
      numBuckets
    )
    Iterator(index)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers
}
