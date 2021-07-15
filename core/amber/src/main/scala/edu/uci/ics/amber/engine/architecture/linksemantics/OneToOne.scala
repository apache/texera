package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{DataSendingPolicy, OneToOnePolicy}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

class OneToOne(from: WorkerLayer, to: WorkerLayer, batchSize: Int)
    extends LinkStrategy(from, to, batchSize) {
  override def getPolicies
      : Iterable[(ActorVirtualIdentity, LinkIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])] = {
    assert(from.isBuilt && to.isBuilt && from.numWorkers == to.numWorkers)
    from.identifiers.indices.map(i =>
      (
        from.identifiers(i),
          id,
        OneToOnePolicy(batchSize, Array(to.identifiers(i))),
        Seq(to.identifiers(i))
      )
    )
  }
}
