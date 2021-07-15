package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.tuple.ITuple

case class HashBasedShufflePartitioner(policy: HashBasedShufflePartitioning)
    extends ParallelBatchingPartitioner() {
  override def selectBatchingIndex(tuple: ITuple): Int = {
    val numBuckets = policy.receivers.length

    (policy.hashColumnIndices
      .map(i => tuple.get(i))
      .toList
      .hashCode() % numBuckets + numBuckets) % numBuckets
  }
}
