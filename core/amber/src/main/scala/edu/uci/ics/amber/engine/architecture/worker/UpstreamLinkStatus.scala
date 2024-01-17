package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.{AmberLogging, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, PhysicalLinkIdentity}

import scala.collection.mutable

class UpstreamLinkStatus(val actorId: ActorVirtualIdentity) extends AmberLogging {

  /**
    * The scheduler may not schedule the entire workflow at once. Consider a 2-phase hash join where the first
    * region to be scheduled is the build part of the workflow and the join operator. The hash join workers will
    * only receive the workers from the upstream operator on the build side in `upstreamMap` through
    * `UpdateInputLinkingHandler`. Thus, the hash join worker may wrongly deduce that all inputs are done when
    * the build part completes. Therefore, we have a `allUpstreamLinkIds` to track the number of actual upstream
    * links that a worker receives data from.
    */
  private val upstreamMap =
    new mutable.HashMap[PhysicalLinkIdentity, Set[ActorVirtualIdentity]].withDefaultValue(Set())
  private val upstreamMapReverse =
    new mutable.HashMap[ActorVirtualIdentity, PhysicalLinkIdentity]
  private val endReceivedFromWorkers = new mutable.HashSet[ActorVirtualIdentity]
  private val completedLinkIds = new mutable.HashSet[PhysicalLinkIdentity]()
  private var allUpstreamLinkIds: Set[PhysicalLinkIdentity] = Set.empty

  def setAllUpstreamLinkIds(newSet: Set[PhysicalLinkIdentity]): Unit = {
    this.allUpstreamLinkIds = newSet
  }

  def registerInput(identifier: ActorVirtualIdentity, input: PhysicalLinkIdentity): Unit = {
    assert(
      allUpstreamLinkIds.contains(input),
      "unexpected input link " + input + " for operator " + VirtualIdentityUtils.getPhysicalOpId(
        actorId
      )
    )
    upstreamMap.update(input, upstreamMap(input) + identifier)
    upstreamMapReverse.update(identifier, input)
  }

  def getInputLinkId(identifier: ActorVirtualIdentity): PhysicalLinkIdentity =
    upstreamMapReverse(identifier)

  def markWorkerEOF(identifier: ActorVirtualIdentity): Unit = {
    if (identifier != null) {
      endReceivedFromWorkers.add(identifier)
      val link = upstreamMapReverse(identifier)
      if (upstreamMap(link).subsetOf(endReceivedFromWorkers)) {
        completedLinkIds.add(link)
      }
    }
  }

  def allUncompletedSenders: Set[ActorVirtualIdentity] = {
    upstreamMap.filterKeys(k => !completedLinkIds.contains(k)).values.flatten.toSet
  }

  def isLinkEOF(link: PhysicalLinkIdentity): Boolean = {
    if (link == null) {
      return true // special case for source operator
    }
    completedLinkIds.contains(link)
  }

  def isAllEOF: Boolean = completedLinkIds.equals(allUpstreamLinkIds)
}
