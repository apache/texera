package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

abstract class LinkStrategy(
    val from: WorkerLayer,
    val to: WorkerLayer,
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
