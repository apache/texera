package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable

class UpstreamLinkStatus(allUpstreamLinkIds: Set[LinkIdentity]) {

  /**
    * The scheduler may not schedule the entire workflow at once. Consider a 2-phase hash join where the first
    * region to be scheduled is the build part of the workflow and the join operator. The hash join workers will
    * only receive the workers from the upstream operator on the build side in `upstreamMap` through
    * `UpdateInputLinkingHandler`. Thus, the hash join worker may wrongly deduce that all inputs are done when
    * the build part completes. Therefore, we have a `allUpstreamLinkIds` to track the number of actual upstream
    * links that a worker receives data from.
    */
  private val upstreamMap = new mutable.HashMap[LinkIdentity, mutable.HashSet[ActorVirtualIdentity]]
  private val endReceivedFromWorkers = new mutable.HashSet[ActorVirtualIdentity]
  private val completedLinkIds = new mutable.HashSet[LinkIdentity]()

  def registerInput(identifier: ActorVirtualIdentity, input: LinkIdentity): Unit = {
    upstreamMap.getOrElseUpdate(input, new mutable.HashSet[ActorVirtualIdentity]()).add(identifier)
  }

  def markWorkerEOF(identifier: ActorVirtualIdentity): Unit = {
    if (identifier != null) {
      endReceivedFromWorkers.add(identifier)
    }
  }

  def isLinkEOF(link: LinkIdentity): Boolean = {
    if (link == null) {
      return true // special case for source operator
    }
    if (upstreamMap(link).subsetOf(endReceivedFromWorkers)) {
      completedLinkIds.add(link)
      return true
    }
    false
  }

  def isAllEOF: Boolean = completedLinkIds.equals(allUpstreamLinkIds)
}
