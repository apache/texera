package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  DataSendingPolicy,
  HashBasedShufflePolicy,
  RoundRobinPolicy
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.concurrent.ExecutionContext

class HashBasedShuffle(
    from: WorkerLayer,
    to: WorkerLayer,
    batchSize: Int,
    hashFunc: ITuple => Int
) extends LinkStrategy(from, to, batchSize) {
  override def getPolicies
      : Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])] = {
    assert(from.isBuilt && to.isBuilt)
    from.identifiers.map(x =>
      (
        x,
        new HashBasedShufflePolicy(id, batchSize, hashFunc, to.identifiers),
        to.identifiers.toSeq
      )
    )
  }

}
