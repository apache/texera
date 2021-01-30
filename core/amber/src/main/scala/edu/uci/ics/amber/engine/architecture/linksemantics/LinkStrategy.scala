package edu.uci.ics.amber.engine.architecture.linksemantics

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

abstract class LinkStrategy(
    val from: WorkerLayer,
    val to: WorkerLayer,
    val batchSize: Int
) extends Serializable {

  val id = LinkIdentity(from.id, to.id)

  def totalReceiversCount: Long = to.numWorkers

  private var currentCompletedCount = 0

  def incrementCompletedReceiversCount(): Unit = currentCompletedCount += 1

  def isCompleted: Boolean = currentCompletedCount == totalReceiversCount

  // returns Iterable of (sender, sender's sending policy, set of receivers)
  def getPolicies: Iterable[(ActorVirtualIdentity, DataSendingPolicy, Seq[ActorVirtualIdentity])]
}
