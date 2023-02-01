package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RoundRobinPartitioning
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class RoundRobinPartitioner(partitioning: RoundRobinPartitioning)
    extends ReshapePartitioner(partitioning.batchSize, partitioning.receivers) {
  var roundRobinIndex = 0

  override def getPartition(tuple: ITuple): ActorVirtualIdentity = {
    roundRobinIndex = (roundRobinIndex + 1) % partitioning.receivers.length
    allReceivers(roundRobinIndex)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers
}
