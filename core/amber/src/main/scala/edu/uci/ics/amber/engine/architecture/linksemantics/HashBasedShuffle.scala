package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  Partitioning,
  HashBasedShufflePartitioning
}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

class HashBasedShuffle(
    from: WorkerLayer,
    to: WorkerLayer,
    batchSize: Int,
    hashColumnIndices: Array[Int]
) extends LinkStrategy(from, to, batchSize) {
  override def getPartitioning: Iterable[
    (ActorVirtualIdentity, LinkIdentity, Partitioning, Seq[ActorVirtualIdentity])
  ] = {
    assert(from.isBuilt && to.isBuilt)
    from.identifiers.map(x =>
      (
        x,
        id,
        HashBasedShufflePartitioning(batchSize, to.identifiers, hashColumnIndices),
        to.identifiers.toSeq
      )
    )
  }

}
