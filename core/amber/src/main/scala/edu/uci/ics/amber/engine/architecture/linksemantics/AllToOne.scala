package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  OneToOnePartitioning,
  Partitioning
}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

class AllToOne(from: OpExecConfig, fromPort: Int, to: OpExecConfig, toPort: Int, batchSize: Int)
    extends LinkStrategy(from, fromPort, to, toPort, batchSize) {
  override def getPartitioning: Iterable[
    (ActorVirtualIdentity, LinkIdentity, Partitioning, Seq[ActorVirtualIdentity])
  ] = {
    assert(to.numWorkers == 1)
    val toActor = to.identifiers.head
    from.identifiers.map(x =>
      (x, id, OneToOnePartitioning(batchSize, Array(toActor)), Seq(toActor))
    )
  }

}
