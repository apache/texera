package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RoundRobinPartitioning
import edu.uci.ics.amber.engine.common.tuple.ITuple

case class RoundRobinPartitioner(partitioning: RoundRobinPartitioning)
    extends ParallelBatchingPartitioner(partitioning.batchSize, partitioning.receivers) {
  var roundRobinIndex = 0

  override def selectBatchingIndex(tuple: ITuple): Int = {
    roundRobinIndex = (roundRobinIndex + 1) % partitioning.receivers.length
    roundRobinIndex
  }
}
