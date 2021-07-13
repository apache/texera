package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  DataSendingPolicy,
  OneToOnePolicy
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class AllToOne(from: WorkerLayer, to: WorkerLayer, batchSize: Int)
    extends LinkStrategy(from, to, batchSize) {
  override def getPolicies
      : Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])] = {
    assert(from.isBuilt && to.isBuilt && to.numWorkers == 1)
    val toActor = to.identifiers.head
    from.identifiers.map(x => (x, new OneToOnePolicy(id, batchSize, Array(toActor)), Seq(toActor)))
  }

}
