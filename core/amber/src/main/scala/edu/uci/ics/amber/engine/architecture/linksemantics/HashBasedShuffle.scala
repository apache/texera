package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  DataSendingPolicy,
  HashBasedShufflePolicy
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

class HashBasedShuffle(
    from: WorkerLayer,
    to: WorkerLayer,
    batchSize: Int,
    hashFunc: ITuple => Int
) extends LinkStrategy(from, to, batchSize) {
  override def getPolicies: Iterable[
    (ActorVirtualIdentity, LinkIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])
  ] = {
    assert(from.isBuilt && to.isBuilt)
    from.identifiers.map(x =>
      (
        x,
        id,
        HashBasedShufflePolicy(batchSize, to.identifiers, hashFunc),
        to.identifiers.toSeq
      )
    )
  }

}
