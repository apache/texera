package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  OneToOnePartitioning,
  Partitioning
}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

class AllToOne(_from: OpExecConfig, _to: OpExecConfig, batchSize: Int)
    extends LinkStrategy(_from, _to, batchSize) {
  override def getPartitioning: Iterable[
    (ActorVirtualIdentity, LinkIdentity, Partitioning, Seq[ActorVirtualIdentity])
  ] = {
    assert(from.isBuilt && to.isBuilt && to.numWorkers == 1)
    val toActor = to.identifiers.head
    from.identifiers.map(x =>
      (x, id, OneToOnePartitioning(batchSize, Array(toActor)), Seq(toActor))
    )
  }

}
