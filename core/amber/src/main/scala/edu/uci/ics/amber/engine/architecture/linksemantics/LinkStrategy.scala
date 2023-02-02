package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

abstract class LinkStrategy(
    val from: OpExecConfig,
    val to: OpExecConfig,
    val batchSize: Int
) extends Serializable {

  val id: LinkIdentity = LinkIdentity(from.id, to.id)
  private var currentCompletedCount = 0

  def incrementCompletedReceiversCount(): Unit = currentCompletedCount += 1

  def isCompleted: Boolean = currentCompletedCount == totalReceiversCount

  def totalReceiversCount: Long = to.numWorkers

  // returns Iterable of (sender, link id, sender's partitioning, set of receivers)
  def getPartitioning
      : Iterable[(ActorVirtualIdentity, LinkIdentity, Partitioning, Seq[ActorVirtualIdentity])]
}
