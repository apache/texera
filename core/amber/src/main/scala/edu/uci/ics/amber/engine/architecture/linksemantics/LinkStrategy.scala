package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

abstract class LinkStrategy(
    val from: WorkerLayer,
    val to: WorkerLayer,
    val batchSize: Int,
    val inputNum: Int
) extends Serializable {

  val tag = LinkIdentity(from.id, to.id, inputNum)

  def getPolicies: Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])]
}
