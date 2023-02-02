package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  OneToOnePartitioning,
  Partitioning
}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

class OneToOne(_from: OpExecConfig, _to: OpExecConfig, batchSize: Int)
    extends LinkStrategy(_from, _to, batchSize) {
  override def getPartitioning: Iterable[
    (ActorVirtualIdentity, LinkIdentity, Partitioning, Seq[ActorVirtualIdentity])
  ] = {
    assert(from.isBuilt && to.isBuilt && from.numWorkers == to.numWorkers)
    from.identifiers.indices.map(i =>
      (
        from.identifiers(i),
        id,
        OneToOnePartitioning(batchSize, Array(to.identifiers(i))),
        Seq(to.identifiers(i))
      )
    )
  }
}
