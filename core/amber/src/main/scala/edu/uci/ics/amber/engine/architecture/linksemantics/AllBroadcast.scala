package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.{
  BroadcastPartitioning,
  Partitioning
}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

class AllBroadcast(
    from: OpExecConfig,
    fromPort: Int,
    to: OpExecConfig,
    toPort: Int,
    batchSize: Int
) extends LinkStrategy(from, fromPort, to, toPort, batchSize) {
  override def getPartitioning: Iterable[
    (ActorVirtualIdentity, LinkIdentity, Partitioning, Seq[ActorVirtualIdentity])
  ] = {
    from.identifiers.map(x =>
      (x, id, BroadcastPartitioning(batchSize, to.identifiers), to.identifiers.toSeq)
    )
  }

}
