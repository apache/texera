package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.HashBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.tuple.ITuple

case class HashBasedShufflePartitioner(partitioning: HashBasedShufflePartitioning)
    extends ParallelBatchingPartitioner(partitioning.batchSize, partitioning.receivers) {
  override def selectBatchingIndex(tuple: ITuple): Int = {
    val numBuckets = partitioning.receivers.length

    (partitioning.hashColumnIndices
      .map(i => tuple.get(i))
      .toList
      .hashCode() % numBuckets + numBuckets) % numBuckets
  }
}
